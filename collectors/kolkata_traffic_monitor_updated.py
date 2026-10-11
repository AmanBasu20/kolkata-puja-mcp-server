"""Kolkata Real-Time Road Congestion Monitor (script version).

Pulls live traffic flow data from the TomTom Traffic API for the road at each
Kolkata metro station, computes a congestion score, and adds an `area_busyness`
level to every station based on road traffic around it.

    congestion % = (1 - current speed / free-flow speed) * 100

Location records are loaded from the project's current JSON datasets. A dry run
is the default; a live collection requires the --live flag and TOMTOM_API_KEY.
"""
import argparse
import math
import os
import time
import json
from collections import Counter
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

import pandas as pd
import requests
from tqdm import tqdm

IST = ZoneInfo("Asia/Kolkata")

# ---------------------------------------------------------------------------
# API key
# ---------------------------------------------------------------------------
# Set TOMTOM_API_KEY in the environment before a live run. Do not prompt
# interactively: scheduled jobs must be able to run unattended.
API_KEY = os.environ.get("TOMTOM_API_KEY", "").strip()

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
# (upper bound in %, label), checked in order
STATUS_BANDS = [
    (15, "Free flow"),
    (35, "Moderate"),
    (60, "Heavy"),
    (101, "Severe"),
]

BASE_URL = "https://api.tomtom.com/traffic/services/4/flowSegmentData/absolute/10/json"

REQUEST_PAUSE_S = 0.25  # pause between actual API requests

# This script is expected to live in <project-root>/collectors/.
PROJECT_ROOT = Path(__file__).resolve().parents[1]
STATIC_DIR = PROJECT_ROOT / "data" / "static"
DYNAMIC_DIR = PROJECT_ROOT / "data" / "dynamic"
PANDALS_FILE = STATIC_DIR / "pandals_2026.json"
STATIONS_FILE = STATIC_DIR / "metro_stations.json"
ENRICHED_PANDALS_FILE = DYNAMIC_DIR / "enriched_pandals.json"
ENRICHED_STATIONS_FILE = DYNAMIC_DIR / "enriched_stations.json"
REFRESH_STATUS_FILE = DYNAMIC_DIR / "crowd_refresh_status.json"
PILOT_RESULTS_FILE = DYNAMIC_DIR / "crowd_pilot_results.json"

# In-run cache: identical rounded coordinates only require one TomTom call.
_FLOW_CACHE = {}
_API_REQUEST_COUNT = 0
_MAX_REQUESTS_ALLOWED = 1250


class RequestBudgetExceeded(RuntimeError):
    """Raised before an API call would exceed the configured request budget."""


# Area busyness settings
RADIUS_M = 700                 # distance of the surrounding sample points
BEARINGS = [0, 90, 180, 270]   # north, east, south, west
INCLUDE_CENTER = True          # also sample the station itself
LIMIT = None                   # e.g. 5 for a quick test, None for all stations

# (upper bound of average congestion in %, level), checked in order
AREA_BANDS = [
    (15, "Light"),
    (35, "Moderate"),
    (60, "Busy"),
    (101, "Very busy"),
]

def load_location_dataset(path, dataset_label):
    """Load the project's authoritative location JSON and validate coordinates."""
    with path.open("r", encoding="utf-8") as f:
        data = json.load(f)
    # Accept either a top-level list or a common object wrapper.
    if isinstance(data, dict):
        for key in ("records", "stations", "pandals", "data"):
            if isinstance(data.get(key), list):
                data = data[key]
                break
    if not isinstance(data, list):
        raise ValueError(f"{path} must contain a JSON list of {dataset_label} records")
    cleaned = []
    invalid = []
    for index, item in enumerate(data):
        if not isinstance(item, dict):
            invalid.append(f"row {index}: not an object")
            continue
        required = ("id", "name", "latitude", "longitude")
        missing = [field for field in required if item.get(field) in (None, "")]
        if missing:
            invalid.append(f"row {index} ({item.get('name', 'unnamed')}): missing {', '.join(missing)}")
            continue
        try:
            item = dict(item)
            item["latitude"] = float(item["latitude"])
            item["longitude"] = float(item["longitude"])
            if not (-90 <= item["latitude"] <= 90 and -180 <= item["longitude"] <= 180):
                raise ValueError("coordinate out of range")
        except (TypeError, ValueError) as exc:
            invalid.append(f"row {index} ({item.get('name', 'unnamed')}): invalid coordinates ({exc})")
            continue
        cleaned.append(item)
    if invalid:
        preview = "\n".join(invalid[:10])
        more = f"\n... and {len(invalid) - 10} more" if len(invalid) > 10 else ""
        raise ValueError(f"Invalid {dataset_label} records in {path}:\n{preview}{more}")
    ids = [item["id"] for item in cleaned]
    if len(ids) != len(set(ids)):
        raise ValueError(f"Duplicate IDs found in {path}")
    return cleaned


def load_project_datasets():
    """Use the project's current master pandal and Metro station datasets."""
    loaded_stations = load_location_dataset(STATIONS_FILE, "Metro station")
    loaded_pandals = load_location_dataset(PANDALS_FILE, "pandal")
    return loaded_stations, loaded_pandals


stations, pandals = load_project_datasets()
print(f"Loaded {len(stations)} Metro stations from {STATIONS_FILE.relative_to(PROJECT_ROOT)}.")
print(f"Loaded {len(pandals)} pandals from {PANDALS_FILE.relative_to(PROJECT_ROOT)}.")

bad_stations = [s.get("name", s.get("id")) for s in stations
                if "latitude" not in s or "longitude" not in s]
assert not bad_stations, f"These stations have no latitude/longitude: {bad_stations}"
bad_pandals = [p.get("name", p.get("id")) for p in pandals
               if "latitude" not in p or "longitude" not in p]
assert not bad_pandals, f"These pandals have no latitude/longitude: {bad_pandals}"

zones = Counter(p.get("zone", "Unspecified") for p in pandals)
print("Pandal records by zone: " + ", ".join(f"{zone} {count}" for zone, count in zones.most_common()))
spot = lambda obj: (round(obj["latitude"], 5), round(obj["longitude"], 5))
pair_counts = Counter(spot(p) for p in pandals)
station_spots = {spot(station) for station in stations}
shared = sum(count for count in pair_counts.values() if count > 1)
on_station = sum(spot(p) in station_spots for p in pandals)
print(f"{len(pair_counts)} distinct coordinate pairs for {len(pandals)} pandals; "
      f"{shared} share a pair with another pandal, {on_station} sit exactly on a Metro station's coordinates.")


# ---------------------------------------------------------------------------
# Helper functions
# ---------------------------------------------------------------------------
def status_label(congestion_pct, closed=False):
    """Turn a congestion percentage into a human-readable status."""
    if closed:
        return "Closed"
    for upper, label in STATUS_BANDS:
        if congestion_pct < upper:
            return label
    return STATUS_BANDS[-1][1]


def fetch_flow(lat, lon):
    """Return TomTom flow data for the road nearest to (lat, lon)."""
    if not API_KEY:
        raise RuntimeError("TOMTOM_API_KEY is not set. Set it before a live run.")
    params = {"point": f"{lat},{lon}", "unit": "KMPH", "key": API_KEY}
    resp = requests.get(BASE_URL, params=params, timeout=15)
    if resp.status_code in (401, 403):
        raise PermissionError("TomTom rejected the key. Check it is correct and the Traffic API is enabled.")
    if resp.status_code == 429:
        raise RuntimeError("Rate limit reached. Poll less often or check your quota.")
    resp.raise_for_status()
    return resp.json()["flowSegmentData"]


def fetch_flow_retry(lat, lon, tries=4):
    """Fetch flow data with an in-run coordinate cache and 429 back-off."""
    global _API_REQUEST_COUNT
    # Five decimal places is the cache key used by the dry-run estimator too.
    key = (round(float(lat), 5), round(float(lon), 5))
    if key in _FLOW_CACHE:
        return _FLOW_CACHE[key]
    for attempt in range(tries):
        if _API_REQUEST_COUNT >= _MAX_REQUESTS_ALLOWED:
            raise RequestBudgetExceeded(
                f"Per-run HTTP request budget ({_MAX_REQUESTS_ALLOWED}) reached; stopping before another request."
            )
        try:
            # Count each HTTP attempt conservatively, including retries/failures,
            # so the reported number does not understate possible quota usage.
            _API_REQUEST_COUNT += 1
            result = fetch_flow(key[0], key[1])
            _FLOW_CACHE[key] = result
            time.sleep(REQUEST_PAUSE_S)
            return result
        except (PermissionError, RequestBudgetExceeded):
            raise
        except Exception:
            if attempt == tries - 1:
                raise
            time.sleep(2 ** attempt)


def read_all(stations):
    """Take one reading at every station and return a DataFrame."""
    now = datetime.now(IST).replace(microsecond=0)
    rows = []
    for s in tqdm(stations):
        name, lat, lon = s["name"], s["latitude"], s["longitude"]
        try:
            d = fetch_flow_retry(lat, lon)
        except PermissionError:
            raise  # a bad key will fail everywhere, so stop immediately
        except Exception as e:
            print(f"  ! {name}: {e}")
            time.sleep(REQUEST_PAUSE_S)
            continue
        time.sleep(REQUEST_PAUSE_S)

        current, free = d["currentSpeed"], d["freeFlowSpeed"]
        congestion = max(0.0, (1 - current / free) * 100) if free else 0.0
        closed = bool(d.get("roadClosure", False))
        rows.append({
            "timestamp": now,
            "location": name,
            "current_speed_kmph": current,
            "free_flow_speed_kmph": free,
            "congestion_pct": round(congestion, 1),
            "status": status_label(congestion, closed),
            "current_travel_time_s": d.get("currentTravelTime"),
            "free_flow_travel_time_s": d.get("freeFlowTravelTime"),
            "confidence": d.get("confidence"),
            "road_closure": closed,
        })
    return pd.DataFrame(rows)


# ---------------------------------------------------------------------------
# Area busyness helpers
# ---------------------------------------------------------------------------
def offset_point(lat, lon, bearing_deg, dist_m):
    """Return the (lat, lon) that lies dist_m metres from the start along a compass bearing."""
    b = math.radians(bearing_deg)
    dlat = (dist_m * math.cos(b)) / 111_320
    dlon = (dist_m * math.sin(b)) / (111_320 * math.cos(math.radians(lat)))
    return lat + dlat, lon + dlon


def sample_points(lat, lon, RADIUS_M=RADIUS_M):
    """The station itself (optional) plus one point per bearing, RADIUS_M metres away."""
    pts = [(lat, lon)] if INCLUDE_CENTER else []
    pts += [offset_point(lat, lon, b, RADIUS_M) for b in BEARINGS]
    return pts


def area_label(avg_congestion):
    for upper, label in AREA_BANDS:
        if avg_congestion < upper:
            return label
    return AREA_BANDS[-1][1]


def segment_key(d):
    """Identify the road segment TomTom returned, so one road is not counted twice."""
    try:
        pts = d["coordinates"]["coordinate"]
        return (round(pts[0]["latitude"], 5), round(pts[0]["longitude"], 5),
                round(pts[-1]["latitude"], 5), round(pts[-1]["longitude"], 5))
    except (KeyError, IndexError, TypeError):
        return None


def measure_area(station,RADIUS_M):
    """Sample the roads around one station and summarise how busy they are."""
    segments = {}  # segment key -> (congestion %, current speed, closed?)
    asked = ok = 0
    for lat, lon in sample_points(station["latitude"], station["longitude"],RADIUS_M):
        asked += 1
        try:
            d = fetch_flow_retry(lat, lon)
        except (PermissionError, RequestBudgetExceeded):
            raise  # a bad key or exhausted request budget must stop the whole run
        except Exception as e:
            print(f"  ! {station.get('name')}: {e}")
            time.sleep(REQUEST_PAUSE_S)
            continue
        ok += 1
        free = d["freeFlowSpeed"]
        cong = max(0.0, (1 - d["currentSpeed"] / free) * 100) if free else 0.0
        key = segment_key(d) or (lat, lon)
        segments.setdefault(key, (cong, d["currentSpeed"], bool(d.get("roadClosure", False))))

    measured_at = datetime.now(IST).replace(microsecond=0).isoformat()
    if not segments:
        return {
            "area_busyness": "Unknown",
            "area_busyness_detail": {
                "road_segments": 0,
                "points_requested": asked,
                "points_ok": ok,
                "radius_m": RADIUS_M,
                "measured_at": measured_at,
            },
        }

    congs = [c for c, _, _ in segments.values()]
    speeds = [s for _, s, _ in segments.values()]
    avg = sum(congs) / len(congs)
    return {
        "area_busyness": area_label(avg),
        "area_busyness_detail": {
            "avg_congestion_pct": round(avg, 1),
            "peak_congestion_pct": round(max(congs), 1),
            "avg_speed_kmph": round(sum(speeds) / len(speeds), 1),
            "road_segments": len(segments),
            "points_requested": asked,
            "points_ok": ok,
            "radius_m": RADIUS_M,
            "road_closure_nearby": any(closed for _, _, closed in segments.values()),
            "measured_at": measured_at,
        },
    }


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def get_area_busyness(locations, radius_m, label):
    """Measure road-traffic-derived busyness for a list of pandals or stations."""
    enriched = []
    for i, location in enumerate(locations, 1):
        result = measure_area(location, radius_m)
        enriched_item = {**location, **result}
        enriched.append(enriched_item)
        detail = result["area_busyness_detail"]
        avg = f"{detail['avg_congestion_pct']:>5}% avg" if "avg_congestion_pct" in detail else "no data"
        print(f"[{i:>3}/{len(locations)}] {location.get('name', '?'):<32} "
              f"{result['area_busyness']:<10} {avg}")
    summary = pd.DataFrame([
        {
            "location": item.get("name"),
            "area_busyness": item["area_busyness"],
            "avg_congestion_pct": item.get("area_busyness_detail", {}).get("avg_congestion_pct"),
            "peak_congestion_pct": item.get("area_busyness_detail", {}).get("peak_congestion_pct"),
            "avg_speed_kmph": item.get("area_busyness_detail", {}).get("avg_speed_kmph"),
            "road_segments": item.get("area_busyness_detail", {}).get("road_segments"),
        }
        for item in enriched
    ])
    print(f"\n{label} busyness summary:")
    if not summary.empty:
        print(summary["area_busyness"].value_counts().to_string())
        print(summary.sort_values("avg_congestion_pct", ascending=False, na_position="last")
              .reset_index(drop=True).to_string(index=False))
    return enriched


def unique_sample_coordinates(locations, radius_m):
    coords = set()
    raw_count = 0
    for location in locations:
        for lat, lon in sample_points(location["latitude"], location["longitude"], radius_m):
            raw_count += 1
            coords.add((round(float(lat), 5), round(float(lon), 5)))
    return raw_count, coords


def atomic_write_json(path, data):
    """Write a JSON snapshot safely so readers never see a half-written file."""
    path.parent.mkdir(parents=True, exist_ok=True)
    temp_path = path.with_suffix(path.suffix + ".tmp")
    with temp_path.open("w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
        f.write("\n")
    os.replace(temp_path, path)


def main():
    parser = argparse.ArgumentParser(
        description="Update TomTom road-traffic-derived busyness estimates for project pandals and Metro stations."
    )
    parser.add_argument("--dry-run", action="store_true",
                        help="validate source datasets and estimate requests without calling TomTom or writing files")
    parser.add_argument("--live", action="store_true",
                        help="perform a live TomTom collection; full runs update the project's main snapshots")
    parser.add_argument("--pilot", action="store_true",
                        help="select a small subset; live pilot output is saved separately and never overwrites main snapshots")
    parser.add_argument("--pilot-stations", type=int, default=2,
                        help="number of Metro stations to measure in pilot mode (default: 2)")
    parser.add_argument("--pilot-pandals", type=int, default=3,
                        help="number of pandals to measure in pilot mode (default: 3)")
    parser.add_argument("--max-requests", type=int, default=None,
                        help="abort before calling TomTom if estimated unique coordinates exceed this limit; default 30 in pilot mode, 1250 otherwise")
    args = parser.parse_args()

    if args.pilot_stations < 1 or args.pilot_pandals < 1:
        parser.error("--pilot-stations and --pilot-pandals must both be at least 1")
    if args.max_requests is None:
        max_requests = 30 if args.pilot else 1250
    else:
        max_requests = args.max_requests
    if max_requests < 1:
        parser.error("--max-requests must be at least 1")

    global _MAX_REQUESTS_ALLOWED
    _MAX_REQUESTS_ALLOWED = max_requests

    active_stations = stations[:args.pilot_stations] if args.pilot else stations
    active_pandals = pandals[:args.pilot_pandals] if args.pilot else pandals
    raw_stations, station_coords = unique_sample_coordinates(active_stations, 100)
    raw_pandals, pandal_coords = unique_sample_coordinates(active_pandals, 500)
    all_coords = station_coords | pandal_coords

    print("\nDRY RUN — no TomTom requests made and no files written." if args.dry_run or not args.live
          else ("\nLIVE PILOT requested." if args.pilot else "\nLIVE FULL RUN requested."))
    print(f"Source pandals available: {len(pandals)} ({PANDALS_FILE.relative_to(PROJECT_ROOT)})")
    print(f"Source Metro stations available: {len(stations)} ({STATIONS_FILE.relative_to(PROJECT_ROOT)})")
    if args.pilot:
        print(f"Pilot selection: {len(active_stations)} Metro stations, {len(active_pandals)} pandals")
    else:
        print("Selected for collection: all Metro stations and pandals")
    print(f"Raw sample-point count for this run: {raw_stations + raw_pandals}")
    print(f"Estimated unique TomTom coordinates after in-run cache: {len(all_coords)}")
    print(f"Configured maximum requests per run: {max_requests}")

    if len(all_coords) > max_requests:
        raise SystemExit("Request estimate exceeds --max-requests. No API calls were made and no files were written.")

    if not args.live:
        if args.pilot:
            print("Pilot dry-run validation passed. No API calls or file writes were made.")
            print("When ready, rerun the same command with --live to test a small subset.")
        else:
            print("Dry-run validation passed. Add --live only after checking the quota and selecting an appropriate refresh plan.")
        return
    if not API_KEY:
        raise SystemExit("TOMTOM_API_KEY is not set. Set it in this PowerShell session before using --live.")

    print("\nMeasuring road-traffic-derived busyness near Metro stations (100 m radius)...")
    enriched_stations = get_area_busyness(active_stations, 100, "Metro station")
    print("\nMeasuring road-traffic-derived busyness near pandals (500 m radius)...")
    enriched_pandals = get_area_busyness(active_pandals, 500, "Pandal")

    completed_at = datetime.now(IST).replace(microsecond=0).isoformat()
    status = {
        "dataset_name": "Kolkata Road-Traffic-Derived Area Busyness Pilot" if args.pilot else "Kolkata Road-Traffic-Derived Area Busyness",
        "last_collected_at": completed_at,
        "source": "TomTom Traffic Flow Segment Data",
        "method": "Average congestion across sampled nearby road segments",
        "note": "This is a road-traffic-derived busyness estimate, not a direct pedestrian crowd or Metro passenger measurement.",
        "api_requests_attempted": _API_REQUEST_COUNT,
        "pilot_run": bool(args.pilot),
        "pandals_count": len(enriched_pandals),
        "metro_stations_count": len(enriched_stations),
    }

    if args.pilot:
        # Keep pilot results separate to avoid replacing full project snapshots
        # with only a handful of locations.
        pilot_payload = {
            **status,
            "pandals": enriched_pandals,
            "metro_stations": enriched_stations,
        }
        atomic_write_json(PILOT_RESULTS_FILE, pilot_payload)
        print("\nPilot collection completed. Main project snapshots were not changed.")
        print(f"TomTom HTTP attempts (including retries/failures): {_API_REQUEST_COUNT}")
        print(f"Saved pilot result: {PILOT_RESULTS_FILE.relative_to(PROJECT_ROOT)}")
    else:
        # Only overwrite the main snapshots after both full collections return.
        atomic_write_json(ENRICHED_STATIONS_FILE, enriched_stations)
        atomic_write_json(ENRICHED_PANDALS_FILE, enriched_pandals)
        atomic_write_json(REFRESH_STATUS_FILE, status)
        print("\nFull collection completed.")
        print(f"TomTom HTTP attempts (including retries/failures): {_API_REQUEST_COUNT}")
        print(f"Saved: {ENRICHED_STATIONS_FILE.relative_to(PROJECT_ROOT)}")
        print(f"Saved: {ENRICHED_PANDALS_FILE.relative_to(PROJECT_ROOT)}")
        print(f"Saved: {REFRESH_STATUS_FILE.relative_to(PROJECT_ROOT)}")


if __name__ == "__main__":
    main()
