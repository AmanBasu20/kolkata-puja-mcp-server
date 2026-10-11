"""
Kolkata Puja Tourist MCP - Live Traffic Collector (Production Architecture)

Scrapes official Kolkata Traffic Police Facebook posts via Playwright.
Uses canonical and grammatical location extraction, stable Facebook post IDs,
conservative timestamp handling, and a historical ledger that survives fetch
failures. Only recent advisories are exposed as current traffic states.
"""

import json
import re
from datetime import datetime, timezone, timedelta
from pathlib import Path
from urllib.parse import urljoin

from bs4 import BeautifulSoup
from playwright.sync_api import sync_playwright

BASE_DIR = Path(__file__).resolve().parent.parent
OUTPUT_FILE = BASE_DIR / "data" / "dynamic" / "live_traffic_2026.json"

TARGET_PAGES = ["KolkataTrafficPolice"]
OFFICIAL_TAG = "#kolkatatrafficupdate"
TRAFFIC_FRESHNESS_HOURS = 14
FUTURE_TIMESTAMP_TOLERANCE_MINUTES = 5

KNOWN_LOCATIONS = [
    "RR Avenue", "R.R. Avenue", "Howrah Bridge", "Strand Road", "Brabourne Road",
    "Bascule Bridge", "AJC Flyover", "Maa Flyover", "Beckbagan Ramp",
    "Dorina Crossing", "Moulali Crossing", "SN Banerjee Road", "S.N. Banerjee Road",
    "AJC Bose Road", "BB Ganguly Street", "B.B. Ganguly Street", "CR Avenue Crossing",
    "CR Avenue", "C.R. Avenue", "Red Road", "Camac Street", "Allen Park",
    "EM Bypass", "Vidyasagar Setu", "7 Point Crossing", "Park Street",
    "Canning Street", "MG Road", "Diamond Harbour Road", "B.T. Road", "BT Road",
    "Canal East Road", "Canal West Road", "Esplanade", "Sealdah"
]

LOCATION_STOP_WORDS = {
    "traffic", "breakdown", "vehicle", "vehicles", "due", "on", "at", "near",
    "towards", "from", "has", "have", "is", "are", "was", "were", "and"
}

CAUSE_PATTERNS = {
    "procession": [r"\bprocession\b"],
    "programme": [r"\bprogramme\b", r"\bprogram\b"],
    "maintenance_work": [r"\bmaintenance work\b", r"\bmaintenance\b"],
    "breakdown": [r"\bbreak(?:down|ing)\b", r"\bbroken down\b"],
    "accident": [r"\baccident\b", r"\bcollision\b"],
    "road_work": [r"\broad work\b", r"\bconstruction\b", r"\bmetro work\b"],
    "waterlogging": [r"\bwater\s*logging\b", r"\bwaterlogged\b"]
}

INFRASTRUCTURE_SUFFIX = (
    r"Road|Street|Avenue|Bridge|Crossing|Flyover|Ramp|Sarani|Phari|More|"
    r"Bypass|Connector|Junction|Underpass|Expressway|Link"
)


def clean_post_text(raw_text: str) -> str:
    """Strip HTML and common Facebook UI text while retaining hashtags."""
    soup = BeautifulSoup(raw_text, "html.parser")
    text = soup.get_text(" ", strip=True).replace("\xa0", " ")
    text = re.sub(r"^.*?Shared with Public\s*", "", text, flags=re.IGNORECASE)

    for delimiter in (
        "All reactions:", "Like | Comment", "| Like |", "View more comments"
    ):
        if delimiter in text:
            text = text.split(delimiter, 1)[0]

    return re.sub(r"\s+", " ", text).strip()


def normalize_location_name(location: str) -> str:
    return re.sub(r"\s+", " ", location.replace(".", "")).strip()


def _clean_clause_prefix(clause: str) -> str:
    return re.sub(
        r"^\s*Traffic update\s*:-\s*\|*\s*", "", clause, flags=re.IGNORECASE
    ).strip()


def _location_regex(location: str) -> re.Pattern:
    """Build a tolerant regex for names whose initials may include periods."""
    token_patterns = []
    for token in normalize_location_name(location).split():
        if token.isalpha() and len(token) <= 3:
            # Handles RR/R.R., BT/B.T., CR/C.R., and similar abbreviated tokens.
            token_patterns.append(r"".join(re.escape(char) + r"\.?" for char in token))
        else:
            token_patterns.append(re.escape(token))
    pattern = r"(?<![A-Za-z0-9])" + r"\s+".join(token_patterns) + r"(?![A-Za-z0-9])"
    return re.compile(pattern, re.IGNORECASE)


def extract_clause_locations(clause: str) -> list[str]:
    """Return canonical locations plus plausible unmapped infrastructure names."""
    found = set()
    clause_clean = _clean_clause_prefix(clause)
    lower_clause = clause_clean.lower()

    # 1. Known/canonical locations. Prefer longer overlapping names.
    known_matches = []
    for loc in KNOWN_LOCATIONS:
        for match in _location_regex(loc).finditer(clause_clean):
            known_matches.append({
                "start": match.start(),
                "end": match.end(),
                "name": normalize_location_name(loc)
            })

    for candidate in known_matches:
        candidate_length = candidate["end"] - candidate["start"]
        contained = any(
            other["start"] <= candidate["start"]
            and candidate["end"] <= other["end"]
            and (other["end"] - other["start"]) > candidate_length
            for other in known_matches
        )
        if not contained:
            found.add(candidate["name"])

    # 2. Prepositional fallback for unmapped infrastructure names.
    fallback_pattern = re.compile(
        r"\b(?:on|at|near|towards|from)\s+(?:the\s+)?"
        r"([A-Za-z0-9.\-]+(?:\s+[A-Za-z0-9.\-]+){0,2}\s+"
        rf"(?:{INFRASTRUCTURE_SUFFIX}))\b",
        re.IGNORECASE
    )
    for match in fallback_pattern.finditer(clause_clean):
        loc = match.group(1).strip()
        for stop_word in ("towards", "from", "near", "on", "at", "and"):
            stop_match = re.search(fr"\b{re.escape(stop_word)}\b", loc, re.IGNORECASE)
            if stop_match:
                loc = loc[:stop_match.start()].strip()
                break
        if loc and loc.lower() not in {"the road", "the bridge", "this road", "the crossing"}:
            if not any(word in LOCATION_STOP_WORDS for word in loc.lower().split()):
                found.add(normalize_location_name(loc))

    # 3. Direct-subject fallback for a new location at the start of the clause.
    direct_pattern = re.compile(
        r"^(?:the\s+)?([A-Za-z0-9.\-]+(?:\s+[A-Za-z0-9.\-]+){0,2}\s+"
        rf"(?:{INFRASTRUCTURE_SUFFIX}))\b",
        re.IGNORECASE
    )
    direct_match = direct_pattern.match(clause_clean)
    if direct_match:
        loc = direct_match.group(1).strip()
        words = loc.lower().split()
        if (
            loc.lower() not in {"the road", "the bridge", "this road", "the crossing"}
            and not any(word in LOCATION_STOP_WORDS for word in words)
        ):
            found.add(normalize_location_name(loc))

    # 4. Context-aware mid-sentence fallback.
    infrastructure_pattern = re.compile(
        r"\b([A-Za-z0-9.\-]+(?:\s+[A-Za-z0-9.\-]+){0,2}\s+"
        rf"(?:{INFRASTRUCTURE_SUFFIX}))\b"
        r"(?=\s+(?:has|have|is|are|was|were|traffic|towards|from|near|and|due|with|"
        r"causing|caused|closed|open|released|slow|blocked|obstructed)\b)",
        re.IGNORECASE
    )
    for match in infrastructure_pattern.finditer(clause_clean):
        loc = match.group(1).strip()
        if (
            not any(word in LOCATION_STOP_WORDS for word in loc.lower().split())
            and loc.lower() not in {"the road", "the bridge", "this road", "the crossing"}
        ):
            found.add(normalize_location_name(loc))

    return sorted(found)


def extract_affected_locations(clause: str) -> list[str]:
    """Prefer roads explicitly affected by traffic conditions over nearby landmarks.

    Example: in "breakdown on Brabourne Road near Canning Street has obstructed
    traffic on Brabourne Road and Howrah Bridge", Canning Street is a landmark;
    Brabourne Road and Howrah Bridge are the explicitly affected locations.
    """
    clause_clean = _clean_clause_prefix(clause)
    candidates = extract_clause_locations(clause_clean)
    if not candidates:
        return []

    # Direct status sentence: "Red Road ... has been released ...". Its grammatical
    # subject is a stronger signal than other locations mentioned later as landmarks.
    direct_pattern = re.compile(
        r"^(?:the\s+)?([A-Za-z0-9.\-]+(?:\s+[A-Za-z0-9.\-]+){0,2}\s+"
        rf"(?:{INFRASTRUCTURE_SUFFIX}))\b",
        re.IGNORECASE
    )
    direct_match = direct_pattern.match(clause_clean)
    if direct_match:
        subject_pattern = _location_regex(direct_match.group(1))
        subject = normalize_location_name(direct_match.group(1))
        if subject_pattern.search(clause_clean) and subject.lower() not in {
            "the road", "the bridge", "this road", "the crossing"
        }:
            canonical_match = next(
                (name for name in candidates if normalize_location_name(name).lower() == subject.lower()),
                None
            )
            return [canonical_match or subject]

    # An explicit "traffic on/along/at/through X" phrase provides a stronger
    # affected-road signal than an earlier "breakdown near Y" reference.
    traffic_anchor = re.search(
        r"\b(?:vehicular\s+)?traffic\s+(?:on|along|at|over|through)\s+",
        clause_clean,
        re.IGNORECASE
    )
    if traffic_anchor:
        affected = []
        anchor_end = traffic_anchor.end()
        for candidate in candidates:
            for occurrence in _location_regex(candidate).finditer(clause_clean):
                if occurrence.start() < anchor_end:
                    continue
                # A location immediately qualified by near/from/towards is normally
                # a landmark or direction, not part of the affected-road list.
                between = clause_clean[anchor_end:occurrence.start()]
                last_list_boundary = max(
                    between.lower().rfind(" and "),
                    between.rfind(",")
                )
                current_item_prefix = between[last_list_boundary + 5:] if last_list_boundary >= 0 and between[last_list_boundary:last_list_boundary + 5].lower() == " and " else (
                    between[last_list_boundary + 1:] if last_list_boundary >= 0 else between
                )
                if re.search(r"\b(?:near|from|towards)\s+(?:the\s+)?[^,]*$", current_item_prefix, re.IGNORECASE):
                    continue
                affected.append(candidate)
                break
        if affected:
            return sorted(set(affected))

    return candidates


def extract_clause_directions(clause: str) -> dict:
    directions = {}
    clause_lower = clause.lower()

    bound_match = re.search(r"\b(north|south|east|west)\s*-?\s*bound\b", clause_lower)
    if bound_match:
        directions["bound"] = f"{bound_match.group(1)} bound"

    flank_match = re.search(
        r"\b(southern|northern|eastern|western|middle)\s+(flank|part|side)\b",
        clause_lower
    )
    if flank_match:
        directions["flank"] = flank_match.group(0)

    stop_words = {
        "has", "have", "is", "are", "was", "were", "and", "towards",
        "from", "near", "traffic", "due", "with"
    }
    for prep in ("towards", "from", "near"):
        pattern = (
            fr"\b{prep}\s+([A-Za-z0-9.]"
            + r"(?:\s+(?!" + "|".join(stop_words) + r"\b)[A-Za-z0-9.]+){0,3})"
            + r"(?=\s|[,\.#]|$)"
        )
        match = re.search(pattern, clause_lower)
        if match:
            chunk = match.group(1).strip().rstrip(".,#").title()
            if len(chunk) < 40:
                directions[prep] = chunk
    return directions


def extract_clause_status(clause: str) -> str:
    """Return normal, slow, closed, or unknown based on the final status phrase."""
    text = clause.lower()
    found_statuses = []
    patterns = {
        "closed": r"\b(closed|shut|no movement|not open|traffic is stopped|closure)\b",
        "slow": r"\b(traffic is slow|traffic is moving slowly|slow|congestion|heavy traffic|"
                r"slowed|slowed down|partially obstructed)\b",
        "normal": r"\b(plying normally|free to vehicular traffic|free to traffic|"
                  r"released for vehicular traffic|traffic is free|normal|cleared|released)\b"
    }
    for status, pattern in patterns.items():
        for match in re.finditer(pattern, text):
            found_statuses.append({"status": status, "index": match.end()})
    if not found_statuses:
        return "unknown"
    found_statuses.sort(key=lambda item: item["index"], reverse=True)
    return found_statuses[0]["status"]


def extract_clause_cause(clause: str) -> str:
    clause_lower = clause.lower()
    for cause, patterns in CAUSE_PATTERNS.items():
        if any(re.search(pattern, clause_lower) for pattern in patterns):
            return cause
    return "unknown"


def parse_post_clauses(message: str) -> list[dict]:
    # Split sentence ends but avoid splitting dotted road abbreviations like B.T. Road.
    raw_clauses = re.split(
        r"(?:;|(?i:\bwhile\b)|(?<![A-Z])\.(?=\s+[A-Z]))",
        message
    )
    clause_states = []
    for clause in raw_clauses:
        if len(clause.strip()) < 10:
            continue
        locs = extract_affected_locations(clause)
        if not locs:
            continue
        clause_states.append({
            "locations": locs,
            "traffic_status": extract_clause_status(clause),
            "cause": extract_clause_cause(clause),
            "directions": extract_clause_directions(clause),
            "raw_clause": clause.strip()
        })
    return clause_states


def parse_fb_time(time_str: str, scraped_at: datetime) -> str | None:
    """Parse supported Facebook relative labels into explicitly approximate UTC times."""
    value = time_str.lower().strip()

    if value.startswith("yesterday"):
        # Facebook's "Yesterday" label does not establish the post's age in hours.
        # Use a conservative age beyond the 26-hour freshness window so an uncertain
        # date-only label cannot make stale traffic look current.
        return (scraped_at - timedelta(hours=48)).isoformat()
    if value.startswith("just now") or value.startswith("a few seconds ago"):
        return scraped_at.isoformat()

    match_w = re.search(r"(\d+)\s*(?:w|weeks?\s*ago)\b", value)
    if match_w:
        return (scraped_at - timedelta(weeks=int(match_w.group(1)))).isoformat()

    match_d = re.search(r"(\d+)\s*(?:d\b|days?\s*ago\b)", value)
    if match_d:
        return (scraped_at - timedelta(days=int(match_d.group(1)))).isoformat()

    match_h = re.search(r"(\d+)\s*(?:h\b|hrs?\b|hours?\s*ago\b)", value)
    if match_h:
        return (scraped_at - timedelta(hours=int(match_h.group(1)))).isoformat()

    match_m = re.search(r"(\d+)\s*(?:m\b|mins?\b|minutes?\s*ago\b)", value)
    if match_m:
        return (scraped_at - timedelta(minutes=int(match_m.group(1)))).isoformat()

    return None


def _extract_post_identity(href: str) -> tuple[str | None, str | None]:
    """Return a stable post ID and specific URL for common Facebook URL formats."""
    absolute_url = urljoin("https://www.facebook.com/", href)
    path_match = re.search(r"/(?:posts|videos)/([^/?#]+)", absolute_url)
    story_match = re.search(r"[?&]story_fbid=([^&#]+)", absolute_url)
    fbid_match = re.search(r"[?&]fbid=([^&#]+)", absolute_url)

    if path_match:
        return path_match.group(1).strip("/"), absolute_url.split("?", 1)[0]

    if story_match:
        return story_match.group(1).strip("/"), absolute_url

    if "permalink.php" in absolute_url and fbid_match:
        return fbid_match.group(1).strip("/"), absolute_url

    # Some Facebook permalink links expose fbid without the permalink.php path.
    if fbid_match and ("facebook.com" in absolute_url):
        return fbid_match.group(1).strip("/"), absolute_url

    return None, None


def fetch_and_parse_posts(scraped_at: datetime) -> tuple[list[dict], bool]:
    scraped_records = []
    seen_ids = set()
    fetch_success = False

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        try:
            context = browser.new_context(
                user_agent=(
                    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                    "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
                ),
                viewport={"width": 1280, "height": 800},
                locale="en-US",
            )
            page = context.new_page()

            for page_name in TARGET_PAGES:
                url = f"https://www.facebook.com/{page_name}"
                print(f"Scraping official page: {page_name}...")
                try:
                    response = page.goto(url, timeout=35000, wait_until="domcontentloaded")
                    if response is not None and response.status >= 400:
                        print(f"[!] Facebook returned HTTP {response.status} for {page_name}.")
                        continue

                    try:
                        page.locator("div[role='dialog'] div[aria-label='Close']").click(timeout=3000)
                    except Exception:
                        pass

                    for _ in range(4):
                        page.evaluate("window.scrollBy(0, 3000)")
                        page.wait_for_timeout(2500)

                    soup = BeautifulSoup(page.content(), "html.parser")
                    articles = soup.find_all("div", role="article")
                    if articles:
                        fetch_success = True

                    for article in articles:
                        raw_text = article.get_text(separator=" ", strip=True)
                        if OFFICIAL_TAG.lower() not in raw_text.lower():
                            continue
                        message = clean_post_text(raw_text)

                        post_url = None
                        post_id = None
                        posted_at = None
                        timestamp_precision = "unknown"

                        for link in article.find_all("a", href=True):
                            href = link.get("href", "")
                            abbr = link.find("abbr")
                            if abbr and abbr.has_attr("data-utime"):
                                try:
                                    ts = int(abbr["data-utime"])
                                    posted_at = datetime.fromtimestamp(ts, tz=timezone.utc).isoformat()
                                    timestamp_precision = "exact"
                                except (ValueError, TypeError, OverflowError, OSError):
                                    pass

                            # Relative labels are accepted only if the parser understands them.
                            link_text = link.get_text(" ", strip=True)
                            if timestamp_precision != "exact" and link_text:
                                parsed_time = parse_fb_time(link_text, scraped_at)
                                if parsed_time is not None:
                                    posted_at = parsed_time
                                    timestamp_precision = "relative"

                            extracted_id, extracted_url = _extract_post_identity(href)
                            if extracted_id and extracted_url:
                                post_id = extracted_id
                                post_url = extracted_url

                            if posted_at and post_url and post_id:
                                break

                        if not posted_at:
                            print("  [!] Dropping post: Could not extract a reliable timestamp.")
                            continue
                        if not post_url or not post_id:
                            print("  [!] Dropping post: Could not extract a specific Post ID/URL.")
                            continue

                        full_stable_id = f"KP-LIVE-FB-{post_id}"
                        if full_stable_id in seen_ids:
                            continue
                        seen_ids.add(full_stable_id)

                        clause_states = parse_post_clauses(message)
                        if not clause_states:
                            locs = extract_affected_locations(message)
                            if locs:
                                clause_states = [{
                                    "locations": locs,
                                    "traffic_status": extract_clause_status(message),
                                    "cause": extract_clause_cause(message),
                                    "directions": extract_clause_directions(message),
                                    "raw_clause": message
                                }]
                        if not clause_states:
                            continue

                        all_locations = sorted({
                            loc for clause in clause_states for loc in clause["locations"]
                        })
                        scraped_records.append({
                            "id": full_stable_id,
                            "post_id": post_id,
                            "locations": all_locations,
                            "clause_states": clause_states,
                            "message": message,
                            "post_url": post_url,
                            "source_page": page_name,
                            "posted_at": posted_at,
                            "timestamp_precision": timestamp_precision,
                            "collected_at": scraped_at.isoformat(),
                        })
                except Exception as exc:
                    print(f"[!] Scrape error on {page_name}: {exc}")
        finally:
            browser.close()

    return scraped_records, fetch_success


def build_latest_states(
    historical_records: list[dict], now_utc: datetime | None = None
) -> dict:
    """Expose only recent advisories; keep stale events in the historical ledger."""
    if now_utc is None:
        now_utc = datetime.now(timezone.utc)

    latest = {}
    sorted_records = sorted(historical_records, key=lambda item: item.get("posted_at", ""))
    for record in sorted_records:
        posted_at = record.get("posted_at")
        if not posted_at:
            continue
        try:
            posted_dt = datetime.fromisoformat(posted_at.replace("Z", "+00:00"))
            if posted_dt.tzinfo is None:
                posted_dt = posted_dt.replace(tzinfo=timezone.utc)
            else:
                posted_dt = posted_dt.astimezone(timezone.utc)
        except (TypeError, ValueError):
            continue

        age = now_utc.astimezone(timezone.utc) - posted_dt
        if age < -timedelta(minutes=FUTURE_TIMESTAMP_TOLERANCE_MINUTES):
            # A materially future-dated source timestamp is not trustworthy.
            continue
        if age > timedelta(hours=TRAFFIC_FRESHNESS_HOURS):
            continue

        for clause in record.get("clause_states", []):
            for loc in clause.get("locations", []):
                latest[loc] = {
                    "traffic_status": clause.get("traffic_status", "unknown"),
                    "cause": clause.get("cause", "unknown"),
                    "directions": clause.get("directions", {}),
                    "raw_clause": clause.get("raw_clause", ""),
                    "original_message": record.get("message", ""),
                    "posted_at": record["posted_at"],
                    "timestamp_precision": record.get("timestamp_precision", "unknown"),
                    "post_url": record.get("post_url")
                }
    return latest


def _merge_scraped_record(existing: dict, incoming: dict) -> dict:
    """Refresh the current contents of an existing post without resetting relative age."""
    old_posted_at = existing.get("posted_at")
    old_precision = existing.get("timestamp_precision", "unknown")
    incoming_posted_at = incoming.get("posted_at")
    incoming_precision = incoming.get("timestamp_precision", "unknown")

    # Always refresh parsed content so edits to the same Facebook post are reflected.
    merged = dict(existing)
    merged.update(incoming)

    if incoming_precision == "exact" and incoming_posted_at:
        merged["posted_at"] = incoming_posted_at
        merged["timestamp_precision"] = "exact"
    elif old_precision == "exact" and old_posted_at:
        # Never replace a previously exact time with a moving relative-time estimate.
        merged["posted_at"] = old_posted_at
        merged["timestamp_precision"] = "exact"
    else:
        # Keep the first stored relative timestamp so repeated scraping of "1d" or
        # "Yesterday" cannot continually make an old post look newly fresh.
        if old_posted_at:
            merged["posted_at"] = old_posted_at
        elif incoming_posted_at:
            merged["posted_at"] = incoming_posted_at

        if old_precision in {"exact", "relative"}:
            merged["timestamp_precision"] = old_precision
        elif incoming_precision == "relative":
            merged["timestamp_precision"] = "relative"
        else:
            merged["timestamp_precision"] = old_precision if old_precision != "unknown" else incoming_precision

    return merged


def run_collector():
    collected_at = datetime.now(timezone.utc)
    print(f"[{collected_at.isoformat()}] Starting strict-schema traffic extraction...")

    new_records, fetch_success = fetch_and_parse_posts(collected_at)
    if not fetch_success:
        print("\n[!] Facebook fetch failed entirely. Keeping existing JSON unchanged.")
        return

    existing_records = []
    if OUTPUT_FILE.exists():
        try:
            with open(OUTPUT_FILE, "r", encoding="utf-8") as file:
                payload = json.load(file)
                if isinstance(payload, dict):
                    existing_records = payload.get("historical_records", [])
        except (json.JSONDecodeError, OSError) as exc:
            print(f"[!] Could not read existing ledger ({exc}); not overwriting it.")
            return

    merged_dict = {
        record["id"]: record
        for record in existing_records
        if isinstance(record, dict) and record.get("id")
    }
    new_count = 0
    updated_count = 0

    for record in new_records:
        record_id = record["id"]
        existing = merged_dict.get(record_id)
        if existing is None:
            merged_dict[record_id] = record
            new_count += 1
            print(f"  [+] Logged Event: {record['locations']} at {record['posted_at']}")
        else:
            merged_dict[record_id] = _merge_scraped_record(existing, record)
            updated_count += 1

    final_historical = list(merged_dict.values())
    final_historical.sort(key=lambda item: item.get("posted_at", ""), reverse=True)
    latest_states = build_latest_states(final_historical, now_utc=collected_at)

    OUTPUT_FILE.parent.mkdir(parents=True, exist_ok=True)
    output_payload = {
        "dataset_name": "Kolkata Live Traffic Advisories",
        "provenance": f"Official Kolkata Police Facebook Updates ({OFFICIAL_TAG})",
        "last_collected_at": collected_at.isoformat(),
        "historical_events_count": len(final_historical),
        "latest_state_per_location": latest_states,
        "historical_records": final_historical,
    }
    with open(OUTPUT_FILE, "w", encoding="utf-8") as file:
        json.dump(output_payload, file, ensure_ascii=False, indent=2)

    print(
        f"\nSuccess: Retained {len(final_historical)} historical records "
        f"({new_count} new, {updated_count} refreshed)."
    )
    print(f"Derived active statuses for {len(latest_states)} locations.")


if __name__ == "__main__":
    run_collector()
