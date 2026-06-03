#!/usr/bin/env python3
"""
Civic Federal Lead Generator — RSS Scraper
==========================================
Scrapes 30+ federal govtech, defence, and legislative RSS feeds daily.
Maintains a rolling 90-day JSON store with deduplication across runs.
Extracts deadlines from article text where possible.

Setup (one-time):
    pip install feedparser

Run manually:
    python fetch_rfps_rss.py

Schedule daily on Windows (Task Scheduler):
    Action → Start a program
    Program : python
    Arguments: "C:\\path\\to\\fetch_rfps_rss.py"
    Trigger  : Daily at 07:00

Schedule daily on Mac/Linux (cron):
    0 7 * * * /usr/bin/python3 /path/to/fetch_rfps_rss.py
"""

import argparse
import hashlib
import json
import re
import time
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

try:
    import feedparser
except ImportError:
    print("ERROR: feedparser not installed.\nRun: pip install feedparser")
    raise SystemExit(1)


# ── Configuration ─────────────────────────────────────────────────────────────

ROLLING_DAYS   = 90    # keep items for this many days
LOOKBACK_DAYS  = 2     # only fetch articles newer than this (avoids re-processing)
RATE_LIMIT_SEC = 0.3   # pause between requests

# ── Feed catalogue ────────────────────────────────────────────────────────────

GOOGLE_NEWS_QUERIES = [
    # Core product signals
    '"constituent engagement" government',
    '"civic engagement" government platform',
    '"community engagement" government software',
    '"government communications platform"',
    '"public comment platform" government',
    '"resident engagement" government technology',
    '"stakeholder engagement" government software',
    '"digital engagement" federal agency',
    # Procurement-specific
    '"constituent engagement" RFP OR "sources sought" OR solicitation',
    '"civic technology" federal contract OR procurement',
    '"community engagement platform" RFP OR bid OR solicitation',
    '"digital outreach" federal agency contract OR award',
    # Competitor intel (agencies that buy = warm leads)
    'Granicus federal contract OR RFP OR award',
    'Zencity government contract OR award',
    'PublicInput government procurement OR contract',
    '"Bang the Table" government contract',
    'GovQA government contract OR RFP',
    # Legislative branch (rarely on SAM.gov)
    'House Representatives "constituent communications" OR "digital engagement" technology',
    'Senate "constituent engagement" OR "communications platform" technology',
    'Congress "civic engagement" platform contract OR procurement',
    # Key civilian agency buyers
    'GSA "community engagement" OR "constituent engagement" platform contract',
    'HHS "community engagement" technology contract OR RFP',
    'EPA "public comment" OR "community engagement" platform procurement',
    'USDA "stakeholder engagement" OR "community engagement" technology',
    'DHS "community engagement" OR "digital outreach" platform',
    'VA "constituent engagement" OR "community outreach" technology',
    'Treasury "public engagement" OR "community outreach" platform',
    # DoD community/civil affairs
    '"Department of Defense" "community engagement" OR "civil affairs" technology',
    'Army OR Navy OR "Air Force" "community engagement" platform',
    # Independent agencies
    'CFPB "public comment" OR "community engagement" technology',
    'FCC "community engagement" OR "public participation" platform',
    'FEMA "community engagement" OR "public outreach" technology',
]

GOOGLE_NEWS_BASE = (
    "https://news.google.com/rss/search?q={query}&hl=en-US&gl=US&ceid=US:en"
)

DEDICATED_FEEDS = {
    # ── Executive branch / civilian IT ──────────────────────────────────
    "FedScoop":              "https://fedscoop.com/feed/",
    "Nextgov / FCW":         "https://www.nextgov.com/rss/all/",
    "Federal News Network":  "https://federalnewsnetwork.com/feed/",
    "Washington Technology": "https://washingtontechnology.com/rss/all.xml",
    "GCN":                   "https://gcn.com/rss-feeds/all.aspx",
    "MeriTalk":              "https://www.meritalk.com/feed/",
    "Government Executive":  "https://www.govexec.com/rss/technology/",
    "Fedtech Magazine":      "https://fedtechmagazine.com/rss.xml",
    "Government Technology": "https://www.govtech.com/rss/rss.php",
    # ── Department of Defense ────────────────────────────────────────────
    "Defense One":           "https://www.defenseone.com/rss/all/",
    "Breaking Defense":      "https://breakingdefense.com/feed/",
    "Defense News":          "https://www.defensenews.com/arc/outboundfeeds/rss/?outputType=xml",
    "C4ISRNET":              "https://www.c4isrnet.com/arc/outboundfeeds/rss/",
    # ── Legislative branch & politics ───────────────────────────────────
    "The Hill":              "https://thehill.com/feed/",
    "Politico":              "https://www.politico.com/rss/politicopicks.xml",
    "Roll Call":             "https://rollcall.com/feed/",
    # ── Broader gov / policy / SLED adjacent ────────────────────────────
    "Governing Magazine":    "https://www.governing.com/rss/all",
    "Route Fifty":           "https://www.route-fifty.com/feed/",
    "Homeland Security Today":"https://www.hstoday.us/feed/",
}


# ── Scoring config ────────────────────────────────────────────────────────────

HIGH_TERMS = [
    "constituent engagement", "civic engagement platform",
    "community engagement platform", "community engagement software",
    "government communications platform", "resident engagement platform",
    "digital civic engagement", "public comment platform",
    "stakeholder engagement platform", "civic tech",
    "constituent communications", "govtech engagement",
    # Competitor mentions = proven buyer signal
    "granicus", "zencity", "publicinput", "bang the table",
    "seeclickfix", "govqa", "polco", "civicrm",
]

MEDIUM_TERMS = [
    "constituent", "civic participation", "community engagement",
    "resident feedback", "public participation", "digital outreach",
    "government outreach platform", "community input", "public engagement",
    "citizen engagement", "stakeholder engagement", "digital government",
    "government software procurement", "govtech", "smart city",
    "rfp", "sources sought", "solicitation", "contract award",
    "market research notice", "pre-solicitation",
]

FILTER_TERMS = set(HIGH_TERMS + MEDIUM_TERMS)

COMPETITOR_TERMS = {
    "granicus", "zencity", "publicinput", "bang the table",
    "seeclickfix", "govqa", "polco", "civicrm",
}

AGENCY_PATTERNS = [
    (r"\b(GSA|General Services Administration)\b",          "GSA"),
    (r"\b(DHS|Department of Homeland Security)\b",          "DHS"),
    (r"\b(HHS|Health and Human Services)\b",                "HHS"),
    (r"\b(DoD|Department of Defense|Pentagon)\b",           "DoD"),
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
    (r"\b(NIH|National Institutes of Health)\b",            "NIH"),
    (r"\b(CDC)\b",                                          "CDC"),
    (r"\b(FBI)\b",                                          "FBI"),
    (r"\b(State Department|Department of State)\b",         "State Dept"),
    (r"\b(Energy Department|Department of Energy|DOE)\b",   "DOE"),
    (r"\b(Interior Department|Department of Interior)\b",   "Interior"),
    (r"\b(House of Representatives|U\.S\. House|House CAO)\b", "U.S. House"),
    (r"\b(U\.S\. Senate|Senate SAA)\b",                     "U.S. Senate"),
    (r"\b(Congress|Congressional)\b",                       "Congress"),
    (r"\b(White House|OMB|OSTP)\b",                         "White House / OMB"),
    (r"\b(CISA)\b",                                         "CISA"),
    (r"\b(Army|U\.S\. Army)\b",                             "U.S. Army"),
    (r"\b(Navy|U\.S\. Navy)\b",                             "U.S. Navy"),
    (r"\b(Air Force)\b",                                    "Air Force"),
    (r"\b(Marines|Marine Corps)\b",                         "Marine Corps"),
]

# ── Deadline extraction ───────────────────────────────────────────────────────

MONTH_NAMES = (
    "january|february|march|april|may|june|july|august|september|"
    "october|november|december|jan|feb|mar|apr|jun|jul|aug|sep|oct|nov|dec"
)

DEADLINE_PATTERNS = [
    # "due by June 15, 2026" / "due June 15"
    rf'(?:responses?|proposals?|submissions?|white\s+papers?|bids?)\s+(?:are\s+)?due\s+(?:by\s+|on\s+)?'
    rf'((?:{MONTH_NAMES})\s+\d{{1,2}}(?:,?\s*\d{{4}})?)',

    # "deadline: June 15, 2026" / "deadline is June 15"
    rf'deadline[:\s]+(?:is\s+|of\s+)?((?:{MONTH_NAMES})\s+\d{{1,2}}(?:,?\s*\d{{4}})?)',

    # "closes June 15" / "closes on June 15, 2026"
    rf'(?:solicitation\s+)?closes?\s+(?:on\s+)?((?:{MONTH_NAMES})\s+\d{{1,2}}(?:,?\s*\d{{4}})?)',

    # "submit by June 15" / "respond by July 1, 2026"
    rf'(?:submit|respond|reply)\s+(?:by|before)\s+((?:{MONTH_NAMES})\s+\d{{1,2}}(?:,?\s*\d{{4}})?)',

    # Numeric: "due by 06/30/2026" / "deadline 7/1/26"
    r'(?:due|deadline|closes?)\s+(?:by\s+|on\s+)?(\d{1,2}/\d{1,2}/\d{2,4})',
]

DATE_FORMATS = [
    "%B %d, %Y", "%B %d %Y", "%b %d, %Y", "%b %d %Y",
    "%B %d", "%b %d",
    "%m/%d/%Y", "%m/%d/%y",
]

def extract_deadline(text: str) -> str:
    """Try to extract a deadline date from article text. Returns YYYY-MM-DD or ''."""
    text_lower = text.lower()
    for pattern in DEADLINE_PATTERNS:
        m = re.search(pattern, text_lower, re.IGNORECASE)
        if not m:
            continue
        raw = m.group(1).strip()
        for fmt in DATE_FORMATS:
            try:
                dt = datetime.strptime(raw, fmt)
                # No year in format → use current or next year
                if "%Y" not in fmt and "%y" not in fmt:
                    now = datetime.now()
                    dt = dt.replace(year=now.year)
                    if dt.date() < now.date():
                        dt = dt.replace(year=now.year + 1)
                # Sanity check: must be in next 18 months
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

def extract_agency(text: str) -> str:
    for pattern, label in AGENCY_PATTERNS:
        if re.search(pattern, text, re.IGNORECASE):
            return label
    return "Federal Government"

def score(text: str) -> tuple[str, int]:
    low = text.lower()
    pts = sum(3 for t in HIGH_TERMS   if t in low)
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
    return True

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


# ── Rolling data management ───────────────────────────────────────────────────

def load_existing(path: Path) -> dict:
    """Load existing results, pruning anything older than ROLLING_DAYS."""
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


# ── Fetch logic ───────────────────────────────────────────────────────────────

def fetch_google_news(query: str, cutoff: datetime) -> list[dict]:
    import urllib.parse
    url = GOOGLE_NEWS_BASE.format(query=urllib.parse.quote(query))
    try:
        feed = feedparser.parse(url)
        return [
            build_result(e, "Google News", query)
            for e in feed.entries
            if is_recent(e, cutoff) and passes_filter(text_of(e))
        ]
    except Exception as ex:
        print(f"      ⚠  Google News '{query[:45]}': {ex}")
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


# ── Main ──────────────────────────────────────────────────────────────────────

def run_once(args):
    """Single fetch run. Extracted so daemon mode can call it in a loop."""
    out_path = Path(args.out) if args.out else Path(__file__).parent / "rfp_results.json"
    cutoff   = datetime.now(timezone.utc) - timedelta(days=(30 if args.full else args.days))

    print(f"\n📡  Civic Federal Lead Generator  [{datetime.now().strftime('%Y-%m-%d %H:%M')}]")
    print(f"    Lookback : {args.days if not args.full else 30} days  (since {cutoff.strftime('%Y-%m-%d')})")
    print(f"    Output   : {out_path}\n")

    store: dict = {} if args.full else load_existing(out_path)

    def add(batch):
        added = 0
        for r in batch:
            if r["noticeId"] not in store:
                store[r["noticeId"]] = r
                added += 1
        return added

    print(f"🔍  Google News ({len(GOOGLE_NEWS_QUERIES)} queries)...")
    for q in GOOGLE_NEWS_QUERIES:
        n = add(fetch_google_news(q, cutoff))
        if n: print(f"    +{n:>3}  \"{q[:60]}\"")
        time.sleep(RATE_LIMIT_SEC)

    print(f"\n📰  Dedicated feeds ({len(DEDICATED_FEEDS)} sources)...")
    for name, url in DEDICATED_FEEDS.items():
        n = add(fetch_dedicated(name, url, cutoff))
        status = f"+{n}" if n else "  0"
        print(f"    {status:>4}  {name}")
        time.sleep(RATE_LIMIT_SEC)

    results = sorted(
        store.values(),
        key=lambda r: (-r["relevanceScore"], r.get("deadline") or "9999", r.get("postedDate") or "")
    )

    high  = sum(1 for r in results if r["relevance"] == "High")
    med   = sum(1 for r in results if r["relevance"] == "Medium")
    proc  = sum(1 for r in results if r["type"] == "Procurement Signal")
    ddl   = sum(1 for r in results if r.get("deadline"))

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

    print(f"\n✅  {len(results)} leads in store  |  {proc} procurement  |  {high} high  |  {med} medium  |  {ddl} deadlines\n")
    return out_path


def main():
    parser = argparse.ArgumentParser(description="Civic RSS lead scraper (daily)")
    parser.add_argument("--days",   type=int, default=LOOKBACK_DAYS,
                        help=f"Look back N days for new articles (default: {LOOKBACK_DAYS})")
    parser.add_argument("--full",   action="store_true",
                        help="Ignore existing data — re-fetch last 30 days from scratch")
    parser.add_argument("--out",    default=None,
                        help="Output JSON path (default: rfp_results.json next to this script)")
    parser.add_argument("--daemon", action="store_true",
                        help="Run continuously — fetch now, then repeat every 24 hours")
    parser.add_argument("--every",  type=int, default=24,
                        help="Hours between runs in daemon mode (default: 24)")
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
            print(f"💤  Next run at {next_run.strftime('%Y-%m-%d %H:%M')}  (sleeping {args.every}h)\n")
            try:
                time.sleep(interval)
            except KeyboardInterrupt:
                print("\n👋  Daemon stopped.")
                sys.exit(0)
    else:
        run_once(args)


if __name__ == "__main__":
    main()
