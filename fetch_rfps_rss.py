#!/usr/bin/env python3
"""
Civic Lead Generator — State & Local Gov RSS Scraper
=====================================================
Targets state/local government technology sources and uses the GDELT API
(server-friendly, no auth required) instead of Google News RSS.
Maintains a rolling 90-day JSON store with deduplication across runs.

Setup (one-time):
    pip install feedparser

Run manually:
    python fetch_rfps_rss.py

Run with 30-day backfill (first time):
    python fetch_rfps_rss.py --full

Run as daemon (refreshes every 24h):
    python fetch_rfps_rss.py --daemon
"""

import argparse
import hashlib
import json
import re
import sys
import time
import urllib.parse
from datetime import datetime, timedelta, timezone
from pathlib import Path

try:
    import feedparser
except ImportError:
    print("ERROR: feedparser not installed.\nRun: pip install feedparser")
    raise SystemExit(1)


# ── Configuration ─────────────────────────────────────────────────────────────

ROLLING_DAYS   = 90   # keep items for this many days
LOOKBACK_DAYS  = 2    # incremental daily lookback
RATE_LIMIT_SEC = 0.4  # pause between requests (be polite)

# ── GDELT queries (replaces Google News — works from any server IP) ────────────
# GDELT aggregates 65,000+ news sources globally. Free, no auth, no IP blocks.

GDELT_QUERIES = [
    # Core product signals — civic/constituent engagement
    '"constituent engagement" software OR platform government',
    '"civic engagement platform" city OR county OR government',
    '"community engagement software" government OR municipality',
    '"resident engagement" platform government technology',
    '"public participation" platform government RFP OR procurement',
    '"digital town hall" OR "virtual town hall" government platform',
    '"government communications platform" RFP OR solicitation',
    '"public comment platform" government city OR county',
    '"citizen portal" OR "resident portal" government software',
    '"311 platform" OR "311 software" city OR county',
    '"participatory budgeting" platform government',
    '"open government" platform RFP OR procurement OR contract',

    # Procurement signals — state/local
    'city OR county "constituent engagement" RFP OR solicitation OR bid',
    'municipality "community engagement" technology contract OR award',
    'state government "engagement platform" OR "resident portal" procurement',
    '"smart city" engagement platform RFP OR procurement',
    'local government "civic tech" OR "govtech" procurement 2025 OR 2026',
    '"community input" platform city OR county RFP OR contract',
    '"public meeting" OR "town hall" software government contract',

    # Competitor intel — agencies buying from competitors = warm leads
    'Granicus government contract OR RFP OR award OR renewal',
    'Zencity city OR county government contract OR deployment',
    'PublicInput government platform contract OR RFP',
    '"Bang the Table" government engagement contract',
    'Polco government survey platform city OR county',
    'SeeClickFix city government contract OR renewal',
    'GovQA government contract OR RFP',

    # Govtech investment/launch signals
    'govtech "civic engagement" OR "constituent engagement" funding OR launch',
    '"civic tech" startup city OR county government contract',
    'government "digital engagement" platform contract OR award OR RFP',
    'city manager "community engagement" technology platform',
]

GDELT_BASE = (
    "https://api.gdeltproject.org/api/v2/doc/doc"
    "?query={query}&mode=artlist&format=rss&maxrecords=25&sort=datedesc"
    "&startdatetime={start}"
)

# ── Dedicated state/local govtech RSS feeds ───────────────────────────────────

DEDICATED_FEEDS = {
    # ── State & local government technology (primary targets) ────────────
    "StateScoop":               "https://statescoop.com/feed/",
    "Government Technology":    "https://www.govtech.com/rss/rss.php",
    "Route Fifty":              "https://www.route-fifty.com/feed/",
    "Governing Magazine":       "https://www.governing.com/rss/all",
    "Smart Cities Dive":        "https://www.smartcitiesdive.com/feeds/news/",
    "GovLoop":                  "https://www.govloop.com/feed/",
    "American City & County":   "https://www.americancityandcounty.com/feed/",
    "Cities Today":             "https://cities-today.com/feed/",
    "Smart Cities World":       "https://smartcitiesworld.net/rss/all-news",
    "PublicCEO":                "https://www.publicceo.com/feed/",
    "ELGL":                     "https://elgl.org/feed/",
    # ── City/county associations ─────────────────────────────────────────
    "ICMA":                     "https://icma.org/news-rss",
    "NLC (Natl League of Cities)": "https://www.nlc.org/feed/",
    # ── Federal govtech (crossover — agencies also buy civic tech) ────────
    "FedScoop":                 "https://fedscoop.com/feed/",
    "Nextgov / FCW":            "https://www.nextgov.com/rss/all/",
    "Federal News Network":     "https://federalnewsnetwork.com/feed/",
    "Government Executive":     "https://www.govexec.com/rss/technology/",
    "GCN":                      "https://gcn.com/rss-feeds/all.aspx",
    "MeriTalk":                 "https://www.meritalk.com/feed/",
}


# ── Scoring ───────────────────────────────────────────────────────────────────

HIGH_TERMS = [
    # Direct product matches
    "constituent engagement", "civic engagement platform",
    "community engagement platform", "community engagement software",
    "resident engagement platform", "resident portal",
    "government communications platform", "public comment platform",
    "digital civic engagement", "citizen engagement platform",
    "stakeholder engagement platform", "civic tech platform",
    "constituent communications", "digital town hall",
    "virtual town hall", "public participation platform",
    "participatory budgeting", "open government platform",
    "311 platform", "311 software",
    # Competitors = proven buyer signal
    "granicus", "zencity", "publicinput", "bang the table",
    "seeclickfix", "govqa", "polco", "civicrm",
]

MEDIUM_TERMS = [
    "constituent", "civic participation", "community engagement",
    "resident feedback", "public participation", "digital outreach",
    "government outreach", "community input", "public engagement",
    "citizen engagement", "stakeholder engagement", "digital government",
    "smart city", "govtech", "civic tech", "government software",
    "rfp", "sources sought", "solicitation", "contract award",
    "pre-solicitation", "market research notice", "bid opportunity",
    "community portal", "resident app", "government app",
    "open data", "transparency platform", "e-government",
]

FILTER_TERMS = set(HIGH_TERMS + MEDIUM_TERMS)

COMPETITOR_TERMS = {
    "granicus", "zencity", "publicinput", "bang the table",
    "seeclickfix", "govqa", "polco", "civicrm",
}

# ── Agency extraction ─────────────────────────────────────────────────────────

AGENCY_PATTERNS = [
    # Named federal agencies
    (r"\b(GSA|General Services Administration)\b",          "GSA"),
    (r"\b(DHS|Department of Homeland Security)\b",          "DHS"),
    (r"\b(HHS|Health and Human Services)\b",                "HHS"),
    (r"\b(EPA|Environmental Protection Agency)\b",          "EPA"),
    (r"\b(USDA|Department of Agriculture)\b",               "USDA"),
    (r"\b(DOT|Department of Transportation)\b",             "DOT"),
    (r"\b(Treasury|IRS)\b",                                 "Treasury"),
    (r"\b(DOJ|Department of Justice)\b",                    "DOJ"),
    (r"\b(VA|Veterans Affairs)\b",                          "VA"),
    (r"\b(NASA)\b",                                         "NASA"),
    (r"\b(CFPB)\b",                                         "CFPB"),
    (r"\b(FCC)\b",                                          "FCC"),
    (r"\b(FEMA)\b",                                         "FEMA"),
    (r"\b(White House|OMB|OSTP)\b",                         "White House / OMB"),
    (r"\b(CISA)\b",                                         "CISA"),
    (r"\b(Congress|Congressional|House|Senate)\b",          "Congress"),
]

def extract_agency(text: str) -> str:
    low = text.lower()
    # Check named federal agencies
    for pattern, label in AGENCY_PATTERNS:
        if re.search(pattern, text, re.IGNORECASE):
            return label
    # State/local detection (broad)
    if re.search(r'\bcity\s+of\b|\bmunicipal\b|\bmunicipality\b|\bcity\s+council\b|\bcity\s+manager\b', low):
        return "City Government"
    if re.search(r'\bcounty\s+of\b|\bcounty\s+government\b|\bcounty\s+commission\b', low):
        return "County Government"
    if re.search(r'\bstate\s+of\b|\bstate\s+government\b|\bstatewide\b|\bstate\s+agency\b', low):
        return "State Government"
    return "Government"


# ── Deadline extraction ───────────────────────────────────────────────────────

MONTH_NAMES = (
    "january|february|march|april|may|june|july|august|september|"
    "october|november|december|jan|feb|mar|apr|jun|jul|aug|sep|oct|nov|dec"
)

DEADLINE_PATTERNS = [
    rf'(?:responses?|proposals?|submissions?|bids?)\s+(?:are\s+)?due\s+(?:by\s+|on\s+)?'
    rf'((?:{MONTH_NAMES})\s+\d{{1,2}}(?:,?\s*\d{{4}})?)',
    rf'deadline[:\s]+(?:is\s+|of\s+)?((?:{MONTH_NAMES})\s+\d{{1,2}}(?:,?\s*\d{{4}})?)',
    rf'(?:solicitation\s+)?closes?\s+(?:on\s+)?((?:{MONTH_NAMES})\s+\d{{1,2}}(?:,?\s*\d{{4}})?)',
    rf'(?:submit|respond|reply)\s+(?:by|before)\s+((?:{MONTH_NAMES})\s+\d{{1,2}}(?:,?\s*\d{{4}})?)',
    r'(?:due|deadline|closes?)\s+(?:by\s+|on\s+)?(\d{1,2}/\d{1,2}/\d{2,4})',
]

DATE_FORMATS = [
    "%B %d, %Y", "%B %d %Y", "%b %d, %Y", "%b %d %Y",
    "%B %d", "%b %d", "%m/%d/%Y", "%m/%d/%y",
]

def extract_deadline(text: str) -> str:
    text_lower = text.lower()
    for pattern in DEADLINE_PATTERNS:
        m = re.search(pattern, text_lower, re.IGNORECASE)
        if not m:
            continue
        raw = m.group(1).strip()
        for fmt in DATE_FORMATS:
            try:
                dt = datetime.strptime(raw, fmt)
                if "%Y" not in fmt and "%y" not in fmt:
                    now = datetime.now()
                    dt = dt.replace(year=now.year)
                    if dt.date() < now.date():
                        dt = dt.replace(year=now.year + 1)
                if timedelta(0) <= dt - datetime.now() <= timedelta(days=548):
                    return dt.strftime("%Y-%m-%d")
            except ValueError:
                continue
    return ""


# ── Helpers ───────────────────────────────────────────────────────────────────

def url_id(url: str) -> str:
    return hashlib.md5(url.encode()).hexdigest()[:14]

def parse_date(entry) -> str:
    for attr in ("published_parsed", "updated_parsed"):
        t = getattr(entry, attr, None)
        if t:
            try:
                return datetime(*t[:6], tzinfo=timezone.utc).strftime("%Y-%m-%d")
            except Exception:
                pass
    return ""

def text_of(entry) -> str:
    parts = [
        getattr(entry, "title",   "") or "",
        getattr(entry, "summary", "") or "",
    ]
    if hasattr(entry, "content"):
        parts.append(entry.content[0].get("value", "") if entry.content else "")
    return " ".join(parts)

def score(text: str) -> tuple[str, int]:
    low = text.lower()
    pts  = sum(3 for t in HIGH_TERMS   if t in low)
    pts += sum(1 for t in MEDIUM_TERMS if t in low)
    if pts >= 6: return "High",   pts
    if pts >= 2: return "Medium", pts
    return "Low", pts

def passes_filter(text: str) -> bool:
    low = text.lower()
    return any(t in low for t in FILTER_TERMS)

def is_recent(entry, cutoff: datetime) -> bool:
    for attr in ("published_parsed", "updated_parsed"):
        t = getattr(entry, attr, None)
        if t:
            try:
                dt = datetime(*t[:6], tzinfo=timezone.utc)
                return dt >= cutoff.replace(tzinfo=timezone.utc)
            except Exception:
                pass
    return True  # include if we can't determine date

def clean_html(raw: str) -> str:
    return re.sub(r"<[^>]+>", " ", raw or "").strip()[:400]

def classify_type(text: str) -> str:
    low = text.lower()
    if any(w in low for w in ["rfp", "sources sought", "solicitation", "bid ", "pre-solicitation", "market research"]):
        return "Procurement Signal"
    return "News Article"

def build_result(entry, source_name: str, matched_kw: str) -> dict:
    full_text = text_of(entry)
    rel_label, rel_score = score(full_text)
    raw_summary = (
        getattr(entry, "summary", "") or
        (entry.content[0].get("value", "") if hasattr(entry, "content") and entry.content else "")
    )
    return {
        "noticeId":       url_id(getattr(entry, "link", matched_kw + full_text[:40])),
        "title":          getattr(entry, "title", "(no title)").strip(),
        "agency":         extract_agency(full_text),
        "type":           classify_type(full_text),
        "postedDate":     parse_date(entry),
        "deadline":       extract_deadline(full_text),
        "naicsCode":      "",
        "link":           getattr(entry, "link", ""),
        "matchedKeyword": matched_kw,
        "relevance":      rel_label,
        "relevanceScore": rel_score,
        "source":         source_name,
        "summary":        clean_html(raw_summary),
    }


# ── Rolling data store ────────────────────────────────────────────────────────

def load_existing(path: Path) -> dict:
    if not path.exists():
        return {}
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
        cutoff = (datetime.now() - timedelta(days=ROLLING_DAYS)).strftime("%Y-%m-%d")
        kept = {
            r["noticeId"]: r
            for r in data.get("results", [])
            if not r.get("postedDate") or r["postedDate"] >= cutoff
        }
        print(f"   Loaded {len(kept)} existing items (≤{ROLLING_DAYS} days old)")
        return kept
    except Exception as e:
        print(f"   Warning: could not load existing data ({e}) — starting fresh")
        return {}


# ── Fetch functions ───────────────────────────────────────────────────────────

def fetch_gdelt(query: str, cutoff: datetime) -> list[dict]:
    """Fetch from GDELT V2 DOC API — works from any server IP, no auth needed."""
    start = cutoff.strftime("%Y%m%d%H%M%S")
    url = GDELT_BASE.format(
        query=urllib.parse.quote(query),
        start=start,
    )
    try:
        feed = feedparser.parse(url)
        return [
            build_result(e, "GDELT / News", query)
            for e in feed.entries
            if passes_filter(text_of(e))
        ]
    except Exception as ex:
        print(f"      ⚠  GDELT '{query[:50]}': {ex}")
        return []


def fetch_dedicated(name: str, url: str, cutoff: datetime) -> list[dict]:
    try:
        feed = feedparser.parse(url)
        results = []
        for e in feed.entries:
            if not is_recent(e, cutoff):
                continue
            txt = text_of(e)
            if not passes_filter(txt):
                continue
            kw = next((t for t in HIGH_TERMS + MEDIUM_TERMS if t in txt.lower()), "keyword match")
            results.append(build_result(e, name, kw))
        return results
    except Exception as ex:
        print(f"      ⚠  {name}: {ex}")
        return []


# ── Main run logic ────────────────────────────────────────────────────────────

def run_once(args):
    out_path = Path(args.out) if args.out else Path(__file__).parent / "rfp_results.json"
    lookback = 30 if args.full else args.days
    cutoff   = datetime.now(timezone.utc) - timedelta(days=lookback)

    print(f"\n📡  Civic Lead Generator  [{datetime.now().strftime('%Y-%m-%d %H:%M')}]")
    print(f"    Lookback : {lookback} days  (since {cutoff.strftime('%Y-%m-%d')})")
    print(f"    Output   : {out_path}\n")

    store: dict = {} if args.full else load_existing(out_path)

    def add(batch):
        added = 0
        for r in batch:
            if r["noticeId"] not in store:
                store[r["noticeId"]] = r
                added += 1
        return added

    # GDELT news searches
    print(f"🔍  GDELT news search ({len(GDELT_QUERIES)} queries)...")
    for q in GDELT_QUERIES:
        n = add(fetch_gdelt(q, cutoff))
        if n: print(f"    +{n:>3}  \"{q[:65]}\"")
        time.sleep(RATE_LIMIT_SEC)

    # Dedicated govtech feeds
    print(f"\n📰  Dedicated feeds ({len(DEDICATED_FEEDS)} sources)...")
    for name, url in DEDICATED_FEEDS.items():
        n = add(fetch_dedicated(name, url, cutoff))
        status = f"+{n}" if n else "  0"
        print(f"    {status:>4}  {name}")
        time.sleep(RATE_LIMIT_SEC)

    # Sort and save
    results = sorted(
        store.values(),
        key=lambda r: (-r["relevanceScore"], r.get("deadline") or "9999", r.get("postedDate") or "")
    )

    high = sum(1 for r in results if r["relevance"] == "High")
    med  = sum(1 for r in results if r["relevance"] == "Medium")
    proc = sum(1 for r in results if r["type"] == "Procurement Signal")
    ddl  = sum(1 for r in results if r.get("deadline"))

    output = {
        "fetchedAt":    datetime.now().isoformat(),
        "dataSource":   "rss",
        "rollingDays":  ROLLING_DAYS,
        "dateRange": {
            "from": (datetime.now() - timedelta(days=ROLLING_DAYS)).strftime("%Y-%m-%d"),
            "to":   datetime.now().strftime("%Y-%m-%d"),
        },
        "totalResults": len(results),
        "results":      results,
    }

    out_path.write_text(json.dumps(output, indent=2, ensure_ascii=False), encoding="utf-8")

    print(f"\n✅  {len(results)} leads  |  {proc} procurement  |  {high} high  |  {med} medium  |  {ddl} deadlines")
    print(f"    Saved → {out_path}\n")
    return out_path


def main():
    parser = argparse.ArgumentParser(description="Civic state/local gov lead scraper")
    parser.add_argument("--days",   type=int, default=LOOKBACK_DAYS,
                        help=f"Look back N days for new articles (default: {LOOKBACK_DAYS})")
    parser.add_argument("--full",   action="store_true",
                        help="Re-fetch last 30 days from scratch (good for first run)")
    parser.add_argument("--out",    default=None,
                        help="Output JSON path (default: rfp_results.json next to script)")
    parser.add_argument("--daemon", action="store_true",
                        help="Run continuously, refreshing every --every hours")
    parser.add_argument("--every",  type=int, default=24,
                        help="Hours between daemon runs (default: 24)")
    args = parser.parse_args()

    if args.daemon:
        interval = args.every * 3600
        print(f"🔄  Daemon mode — running every {args.every}h  (Ctrl+C to stop)\n")
        run_count = 0
        while True:
            run_count += 1
            print(f"── Run #{run_count} ──────────────────────────────────────────")
            try:
                run_once(args)
            except Exception as e:
                print(f"⚠  Run failed: {e} — will retry next cycle")
            next_run = datetime.now() + timedelta(seconds=interval)
            print(f"💤  Next run at {next_run.strftime('%Y-%m-%d %H:%M')}\n")
            try:
                time.sleep(interval)
            except KeyboardInterrupt:
                print("\n👋  Daemon stopped.")
                sys.exit(0)
    else:
        run_once(args)


if __name__ == "__main__":
    main()
