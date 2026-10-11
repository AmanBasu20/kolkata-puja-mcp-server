#!/usr/bin/env python3
"""
Kolkata Puja Tourist MCP - ScrapeBadger Facebook Dynamic Update Collector

Collects the newest Facebook Page post within a rolling freshness window.
For a selected recent post, it stores the post text/date/URL as the latest
update and resolves attached images through ScrapeBadger's single-post
endpoint when the timeline response only exposes photo.php or small thumbnail
URLs.

Required:
    SCRAPEBADGER_API_KEY

Optional:
    GROQ_API_KEY
    GROQ_MODEL (default qwen/qwen3.8-27b)
    GROQ_BASE_URL (default https://api.groq.com/openai/v1)

Examples:
    python scripts/scrape_pandal_facebook_updates_2026.py --pandal-id P019
    python scripts/scrape_pandal_facebook_updates_2026.py --freshness-days 1
    python scripts/scrape_pandal_facebook_updates_2026.py --limit 5
    python scripts/scrape_pandal_facebook_updates_2026.py --pandal-id P019 --debug-raw
"""

from __future__ import annotations

import argparse
import base64
import hashlib
import html
import json
import os
import re
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple
from urllib.parse import quote, urlencode, urlparse, parse_qsl

import requests

try:
    from openai import OpenAI
except ImportError:  # pragma: no cover
    OpenAI = None

ROOT = Path(__file__).resolve().parents[1]
SOCIAL_FILE = ROOT / "data" / "static" / "official_social_media_pages_2026.json"
LEGACY_SOCIAL_FILE = ROOT / "data" / "static" / "official_pandal_facebook_pages_2026.json"
OUTPUT_FILE = ROOT / "data" / "dynamic" / "pandal_updates_2026.json"
CACHE_FILE = ROOT / "data" / "dynamic" / "facebook_image_validation_cache_2026.json"
RAW_DIR = ROOT / "data" / "dynamic" / "facebook_raw_2026"
IMAGE_DIR = ROOT / "data" / "dynamic" / "facebook_images_2026"

YEAR = 2026
SCRAPEBADGER_BASE_URL = "https://scrapebadger.com/v1/facebook"
REQUEST_TIMEOUT = 60
FRESHNESS_DAYS = 1
MAX_IMAGES_PER_PANDAL = 1
MAX_IMAGE_BYTES = 12 * 1024 * 1024
MAX_IMAGE_URLS_FROM_DETAIL = 10
MAX_VISION_REQUESTS_DEFAULT = 20
VISION_CONFIDENCE_THRESHOLD = 0.80
MAX_API_RETRIES = 6
RATE_LIMIT_BUFFER_SECONDS = 2

GROQ_BASE_URL = os.environ.get("GROQ_BASE_URL", "https://api.groq.com/openai/v1")
GROQ_API_KEY = os.environ.get("GROQ_API_KEY")
VISION_MODEL = os.environ.get("GROQ_MODEL", "qwen/qwen3.8-27b")

SESSION = requests.Session()
SESSION.headers.update({
    "User-Agent": "Kolkata-Puja-Tourist-MCP/1.0",
    "Accept": "application/json",
})


def now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def clean_text(value: Any, limit: int = 3000) -> Optional[str]:
    if value is None:
        return None
    text = re.sub(r"\s+", " ", str(value)).strip()
    return text[:limit] if text else None


def atomic_write(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    tmp.replace(path)


def load_json(path: Path, default: Any) -> Any:
    if not path.exists():
        return default
    try:
        text = path.read_text(encoding="utf-8").strip()
        if not text:
            return default
        return json.loads(text)
    except Exception as exc:
        print(f"WARNING: Could not read {path}: {exc}")
        return default


def normalize_url(url: str) -> str:
    url = (url or "").strip()
    if not url:
        return ""
    if not re.match(r"^https?://", url, re.IGNORECASE):
        url = "https://" + url
    parsed = urlparse(url)
    host = parsed.netloc.lower().split(":", 1)[0]
    if host.startswith("www."):
        host = host[4:]
    if host.startswith("m."):
        host = host[2:]
    path = parsed.path.rstrip("/") or "/"
    return f"https://{host}{path}" + (f"?{parsed.query}" if parsed.query else "")


def is_facebook_url(url: str) -> bool:
    try:
        host = urlparse(normalize_url(url)).netloc.lower()
        return host == "facebook.com" or host.endswith(".facebook.com")
    except Exception:
        return False


def parse_datetime(value: Any) -> Optional[datetime]:
    if value is None:
        return None
    text = str(value).strip()
    if not text:
        return None

    if text.isdigit():
        try:
            number = int(text)
            if number > 10_000_000_000:
                number //= 1000
            if 500_000_000 < number < 5_000_000_000:
                return datetime.fromtimestamp(number, tz=timezone.utc)
        except (ValueError, OSError, OverflowError):
            pass

    try:
        dt = datetime.fromisoformat(text.replace("Z", "+00:00"))
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt
    except ValueError:
        pass

    for fmt in (
        "%Y-%m-%d", "%Y-%m-%d %H:%M:%S", "%Y-%m-%d %H:%M",
        "%d-%m-%Y", "%d/%m/%Y", "%m/%d/%Y",
        "%B %d, %Y", "%b %d, %Y", "%B %d %Y", "%b %d %Y",
    ):
        try:
            return datetime.strptime(text, fmt).replace(tzinfo=timezone.utc)
        except ValueError:
            continue
    return None


def sha256_bytes(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def image_data_url(image_bytes: bytes, content_type: str = "image/jpeg") -> str:
    return f"data:{content_type};base64,{base64.b64encode(image_bytes).decode('ascii')}"


def extract_facebook_identifier(page_url: str) -> str:
    parsed = urlparse(normalize_url(page_url))
    parts = [p for p in parsed.path.strip("/").split("/") if p]
    if not parts:
        raise ValueError(f"Could not derive Facebook Page identifier from {page_url}")
    if len(parts) >= 2 and parts[0].lower() in {"p", "pages"}:
        tail = parts[-1]
        match = re.search(r"(\d{8,})$", tail)
        return match.group(1) if match else tail
    return parts[-1]


def get_nested_value(record: Dict[str, Any], keys: Iterable[str]) -> Any:
    lowered = {str(k).lower(): v for k, v in record.items()}
    for key in keys:
        value = lowered.get(key.lower())
        if value is not None:
            return value
    return None


def direct_image_candidate(url: str) -> bool:
    """Accept CDN/direct image URLs, reject Facebook HTML/photo.php URLs."""
    if not url or is_facebook_url(url):
        return False
    lowered = url.lower()
    return any(token in lowered for token in (".jpg", ".jpeg", ".png", ".webp", ".gif", "fbcdn", "scontent"))


def recursively_find_media(node: Any, found: List[str], seen: set[str]) -> None:
    if isinstance(node, dict):
        for key, value in node.items():
            key_l = str(key).lower()
            if isinstance(value, str) and value.startswith("http"):
                if ("image" in key_l or "photo" in key_l or "picture" in key_l or "media" in key_l
                        or key_l in {"url", "src", "source"}) and direct_image_candidate(value):
                    if value not in seen:
                        seen.add(value)
                        found.append(value)
            recursively_find_media(value, found, seen)
    elif isinstance(node, list):
        for value in node:
            recursively_find_media(value, found, seen)


def normalize_post(raw: Dict[str, Any]) -> Dict[str, Any]:
    post_id = get_nested_value(raw, ("post_id", "postId", "id"))
    post_url = get_nested_value(raw, ("url", "post_url", "postUrl", "permalink_url", "permalink", "link"))
    post_text = get_nested_value(raw, ("text", "message", "content", "body", "caption"))
    post_date = get_nested_value(
        raw,
        ("created_at", "createdAt", "published_at", "publishedAt", "timestamp", "created_time", "createdTime", "date", "time"),
    )

    images: List[str] = []
    recursively_find_media(raw, images, set())
    return {
        "id": str(post_id) if post_id is not None else None,
        "post_url": normalize_url(str(post_url)) if post_url else None,
        "post_text": clean_text(post_text),
        "post_datetime": parse_datetime(post_date),
        "image_urls": images[:MAX_IMAGE_URLS_FROM_DETAIL],
        "raw": raw,
    }


def load_verified_pages() -> Tuple[Dict[str, Dict[str, Any]], Path, int]:
    source_file = SOCIAL_FILE if SOCIAL_FILE.exists() else LEGACY_SOCIAL_FILE
    if not source_file.exists():
        return {}, SOCIAL_FILE, 0
    payload = load_json(source_file, {})
    records = payload.get("records", []) if isinstance(payload, dict) else []
    mapping: Dict[str, Dict[str, Any]] = {}
    verified_count = 0
    for record in records:
        if not isinstance(record, dict) or record.get("facebook_verified") is not True:
            continue
        verified_count += 1
        pandal_id = str(record.get("pandal_id") or record.get("id") or "").strip().upper()
        page_url = normalize_url(str(record.get("facebook_page") or ""))
        if pandal_id and page_url and is_facebook_url(page_url):
            mapping[pandal_id] = {**record, "facebook_page": page_url}
    return mapping, source_file, verified_count


def load_previous_records(output_file: Path) -> Dict[str, Dict[str, Any]]:
    payload = load_json(output_file, {})
    records = payload.get("records", []) if isinstance(payload, dict) else []
    return {str(r["pandal_id"]).upper(): r for r in records if isinstance(r, dict) and r.get("pandal_id")}


def base_record(pandal_id: str, source: Dict[str, Any], freshness_days: int) -> Dict[str, Any]:
    return {
        "pandal_id": pandal_id,
        "name": source.get("pandal_name") or source.get("name") or pandal_id,
        "facebook_page": source.get("facebook_page"),
        "facebook_verified": True,
        "latest_update": None,
        "latest_announcement": None,
        "latest_post_date": None,
        "latest_post_url": None,
        "images": [],
        "pending_images": [],
        "scrape_status": "not_run",
        "last_scraped_at": None,
        "last_error": None,
        "freshness_window_days": freshness_days,
    }


def parse_rate_limit_retry_after(payload: Any, response: requests.Response) -> Optional[float]:
    """Prefer the API's reset_at field; otherwise use Retry-After."""
    reset_at = None
    if isinstance(payload, dict):
        reset_at = payload.get("reset_at")
        if isinstance(payload.get("error"), dict):
            reset_at = payload["error"].get("reset_at") or reset_at
    if reset_at is not None:
        try:
            delay = float(reset_at) - time.time() + RATE_LIMIT_BUFFER_SECONDS
            return max(0.0, delay)
        except (TypeError, ValueError):
            pass
    if isinstance(payload, dict) and payload.get("retry_after") is not None:
        try:
            return max(0.0, float(payload["retry_after"]) + RATE_LIMIT_BUFFER_SECONDS)
        except (TypeError, ValueError):
            pass
    header = response.headers.get("Retry-After")
    if header:
        try:
            return max(0.0, float(header) + RATE_LIMIT_BUFFER_SECONDS)
        except ValueError:
            pass
    return None


def scrapebadger_get(path: str, debug_file: Optional[Path] = None) -> Dict[str, Any]:
    api_key = os.environ.get("SCRAPEBADGER_API_KEY")
    if not api_key:
        return {"status": "config_error", "error": "SCRAPEBADGER_API_KEY is not set"}

    url = f"{SCRAPEBADGER_BASE_URL}{path}"
    last_error = "ScrapeBadger request failed"

    for attempt in range(MAX_API_RETRIES + 1):
        try:
            response = SESSION.get(url, headers={"x-api-key": api_key}, timeout=REQUEST_TIMEOUT)
        except requests.RequestException as exc:
            return {"status": "error", "error": f"ScrapeBadger request failed: {exc}"}

        try:
            payload = response.json()
        except ValueError:
            payload = {"raw_text": response.text or ""}

        if debug_file:
            atomic_write(debug_file, payload)

        if response.status_code in {429, 500, 502, 503, 504}:
            retry_delay = parse_rate_limit_retry_after(payload, response)
            if retry_delay is None:
                retry_delay = min(60.0, 2.0 ** attempt)
            if attempt < MAX_API_RETRIES:
                label = "rate_limited" if response.status_code == 429 else "provider_5xx"
                print(f"  {label} ({response.status_code}) | retry {attempt + 1}/{MAX_API_RETRIES} | waiting {round(retry_delay)}s")
                time.sleep(retry_delay)
                continue
            last_error = str(payload)
            return {"status": "error", "http_status": response.status_code, "error": last_error}

        if response.status_code < 200 or response.status_code >= 300:
            detail = payload.get("error") if isinstance(payload, dict) else None
            if isinstance(payload, dict) and payload.get("message"):
                detail = payload.get("message")
            return {
                "status": "error",
                "http_status": response.status_code,
                "error": str(detail or (response.text or "")[:1000] or "ScrapeBadger returned an error"),
            }

        return {"status": "success", "payload": payload}

    return {"status": "error", "error": last_error}



def scrapebadger_web_scrape(target_url: str, debug_file: Optional[Path] = None) -> Dict[str, Any]:
    """Fallback renderer for public Facebook posts whose native media endpoint
    exposes a video attachment but no poster/image URL.
    """
    api_key = os.environ.get("SCRAPEBADGER_API_KEY")
    if not api_key:
        return {"status": "config_error", "error": "SCRAPEBADGER_API_KEY is not set"}
    if not target_url or not target_url.startswith(("http://", "https://")):
        return {"status": "error", "error": "No valid post URL available for web scrape"}

    endpoint = "https://scrapebadger.com/v1/web/scrape"
    request_body = {
        "url": target_url,
        "format": "html",
        "escalate": True,
        "max_cost": 10,
    }
    try:
        response = SESSION.post(
            endpoint,
            headers={"x-api-key": api_key, "Content-Type": "application/json", "Accept": "application/json"},
            json=request_body,
            timeout=120,
        )
    except requests.RequestException as exc:
        return {"status": "error", "error": f"ScrapeBadger web scrape failed: {exc}"}

    try:
        payload = response.json()
    except ValueError:
        payload = {"raw_text": response.text or ""}

    if debug_file:
        atomic_write(debug_file, payload)

    if response.status_code < 200 or response.status_code >= 300:
        detail = payload.get("error") if isinstance(payload, dict) else None
        return {
            "status": "error",
            "http_status": response.status_code,
            "error": str(detail or (response.text or "")[:1000] or "ScrapeBadger web scrape returned an error"),
        }

    data = payload.get("data") if isinstance(payload, dict) and isinstance(payload.get("data"), dict) else payload
    content = data.get("content") if isinstance(data, dict) else ""
    return {"status": "success", "content": content or "", "payload": payload}


def extract_image_urls_from_html(content: str) -> List[str]:
    """Extract ONLY likely Facebook post-media images from a scraped post/video page.

    Important: Facebook pages contain many generic UI assets under
    static.xx.fbcdn.net. Those are not post media and must never be sent to
    Groq Vision. For the video fallback we therefore require an actual
    scontent*.fbcdn.net media host and an image/media path characteristic of
    Facebook CDN media.
    """
    if not content:
        return []

    decoded = html.unescape(content).replace("\\/", "/")
    ranked: List[Tuple[int, str]] = []
    seen: set[str] = set()

    def is_real_facebook_media(url: str) -> bool:
        try:
            parsed = urlparse(url)
            host = parsed.netloc.lower().split(":", 1)[0]
            path = parsed.path.lower()
        except Exception:
            return False

        # Never accept Facebook's generic UI/static asset host.
        if host.startswith("static.") or host.startswith("external."):
            return False

        # The actual post/video media observed from Facebook is served from
        # scontent*.fbcdn.net. Keep this strict rather than treating every
        # .webp/.jpg URL on a Facebook page as post media.
        if not host.startswith("scontent") or not host.endswith(".fbcdn.net"):
            return False

        if not any(ext in path for ext in (".jpg", ".jpeg", ".png", ".webp", ".gif", ".avif")):
            return False

        # Facebook media paths normally include a versioned /v/t... segment.
        # This filters out miscellaneous CDN resources that happen to be images.
        if "/v/t" not in path:
            return False

        return True

    def add(url: str, score: int) -> None:
        url = html.unescape((url or "").strip()).replace("\\/", "/")
        url = url.strip("'\" \t\r\n,;)")
        if not url.startswith(("http://", "https://")) or url in seen:
            return
        if not is_real_facebook_media(url):
            return
        seen.add(url)
        low = url.lower()
        if "/v/t15." in low:
            score += 80  # common Facebook video media path
        if "/v/t39." in low or "/v/t51." in low:
            score += 50  # common photo/reel media paths
        if "thumbnail" in low or "poster" in low:
            score += 20
        ranked.append((score, url))

    meta_pattern_1 = re.compile(
        r'<meta[^>]+(?:property|name)=["\'](?:og:image|og:image:url|twitter:image|twitter:image:src)["\'][^>]+content=["\']([^"\']+)["\']',
        flags=re.I,
    )
    meta_pattern_2 = re.compile(
        r'<meta[^>]+content=["\']([^"\']+)["\'][^>]+(?:property|name)=["\'](?:og:image|og:image:url|twitter:image|twitter:image:src)["\']',
        flags=re.I,
    )
    for match in meta_pattern_1.finditer(decoded):
        add(match.group(1), 300)
    for match in meta_pattern_2.finditer(decoded):
        add(match.group(1), 300)

    # Secondary fallback: literal Facebook CDN media URLs embedded in JSON/scripts.
    for match in re.finditer(r'https?://[^\s"\'<>\\]+', decoded):
        url = match.group(0).rstrip("'`),]}")
        add(url, 120)

    ranked.sort(key=lambda item: item[0], reverse=True)
    return [url for _, url in ranked[:MAX_IMAGE_URLS_FROM_DETAIL]]


def post_detail_has_url_less_video(detail: Dict[str, Any]) -> bool:
    """Detect a Video attachment with no usable media/poster URL."""
    attachments = detail.get("attachments") if isinstance(detail, dict) else None
    found_video = False
    found_url = False

    def walk(node: Any) -> None:
        nonlocal found_video, found_url
        if isinstance(node, dict):
            type_text = str(node.get("type") or node.get("media_type") or "").lower()
            if type_text == "video" or "video" in type_text:
                found_video = True
            for key, value in node.items():
                key_l = str(key).lower()
                if key_l in {"url", "uri", "src", "source", "href", "thumbnail", "thumbnail_url", "image", "image_url", "photo", "photo_url"}:
                    if isinstance(value, str) and value.startswith(("http://", "https://")):
                        found_url = True
                walk(value)
        elif isinstance(node, list):
            for value in node:
                walk(value)

    walk(attachments)
    return found_video and not found_url

def scrape_page_posts(identifier: str, debug_raw_file: Optional[Path] = None) -> Dict[str, Any]:
    encoded = quote(identifier, safe="")
    result = scrapebadger_get(f"/pages/{encoded}/posts", debug_file=debug_raw_file)
    if result.get("status") != "success":
        return result
    payload = result.get("payload") or {}
    posts = payload.get("posts") if isinstance(payload, dict) else None
    if posts is None and isinstance(payload, dict):
        data = payload.get("data")
        posts = data.get("posts") if isinstance(data, dict) else data
    if posts is None:
        posts = []
    if not isinstance(posts, list):
        return {"status": "error", "error": "ScrapeBadger response did not contain a posts list"}
    return {"status": "success", "posts": [x for x in posts if isinstance(x, dict)]}


def scrape_single_post(post_id: str, debug_file: Optional[Path] = None) -> Dict[str, Any]:
    """Fetch full selected-post details, especially attachments/media."""
    encoded = quote(str(post_id), safe="")
    result = scrapebadger_get(f"/posts/{encoded}", debug_file=debug_file)
    if result.get("status") != "success":
        return result
    payload = result.get("payload") or {}
    if isinstance(payload, dict):
        # Some APIs wrap detail in a data/post field; normalize whichever is present.
        detail = payload.get("post") or payload.get("data") or payload
    else:
        detail = payload
    return {"status": "success", "post": detail if isinstance(detail, dict) else {}}


def expand_facebook_thumbnail_url(url: str) -> List[str]:
    """Return the original Facebook CDN URL plus larger-size variants.

    ScrapeBadger can expose the selected post's attachment as a Facebook CDN
    thumbnail such as ``ctp=s50x50``. That is a real image URL, but it is too
    small for Vision. The same signed CDN URL can often be requested at a
    larger preset by changing only the size selector.
    """
    variants = [url]
    try:
        parsed = urlparse(url)
        query = parse_qsl(parsed.query, keep_blank_values=True)
        params = dict(query)
        ctp = params.get("ctp", "")
        if ctp.startswith("s") and re.fullmatch(r"s\d+x\d+", ctp):
            for size in ("p720x720", "p1080x1080"):
                larger = dict(params)
                larger["ctp"] = size
                rebuilt = parsed._replace(query=urlencode(larger, doseq=True)).geturl()
                if rebuilt not in variants:
                    variants.append(rebuilt)
    except Exception:
        pass
    return variants


def download_image(url: str) -> Tuple[Optional[bytes], Optional[str], Optional[str]]:
    headers = {
        "Accept": "image/avif,image/webp,image/apng,image/svg+xml,image/*,*/*;q=0.8",
        "Referer": "https://www.facebook.com/",
    }
    last_error: Optional[str] = None
    last_content_type: Optional[str] = None

    for candidate_url in expand_facebook_thumbnail_url(url):
        try:
            response = SESSION.get(candidate_url, headers=headers, timeout=REQUEST_TIMEOUT, stream=True)
        except requests.RequestException as exc:
            last_error = str(exc)
            continue
        if response.status_code >= 400:
            last_error = f"HTTP {response.status_code}"
            response.close()
            continue
        content_type = response.headers.get("Content-Type", "").lower()
        last_content_type = content_type or last_content_type
        if content_type and not content_type.startswith("image/"):
            last_error = f"Not an image: {content_type}"
            response.close()
            continue
        chunks: List[bytes] = []
        total = 0
        try:
            for chunk in response.iter_content(chunk_size=64 * 1024):
                if not chunk:
                    continue
                total += len(chunk)
                if total > MAX_IMAGE_BYTES:
                    last_error = f"Image exceeds {MAX_IMAGE_BYTES} bytes"
                    chunks = []
                    break
                chunks.append(chunk)
        finally:
            response.close()
        data = b"".join(chunks)
        if len(data) < 10_000:
            last_error = "Image too small"
            continue
        return data, None, content_type

    return None, last_error or "Unable to download image", last_content_type


def get_vision_client() -> Optional[Any]:
    if not GROQ_API_KEY or OpenAI is None:
        return None
    try:
        return OpenAI(api_key=GROQ_API_KEY, base_url=GROQ_BASE_URL)
    except Exception:
        return None


def validate_image_with_vision(client: Any, image_bytes: bytes, context: str) -> Dict[str, Any]:
    prompt = f"""
You are validating an image for a Kolkata Durga Puja tourism dataset.

Pandal context:
{context}

Determine whether the image is actually a photograph of a Durga Puja pandal,
Durga idol/mandap, puja decoration/entrance, or a clear view of the puja venue.
Reject sponsor advertisements, committee portraits, selfies, generic people-only
photos, generic road/city photos, logos, posters, flyers, QR codes, screenshots,
memes, or unrelated celebrations.

Return JSON only:
{{
  "is_pandal_image": true or false,
  "confidence": number between 0 and 1,
  "reason": "brief reason"
}}
""".strip()
    try:
        response = client.chat.completions.create(
            model=VISION_MODEL,
            messages=[{"role": "user", "content": [
                {"type": "text", "text": prompt},
                {"type": "image_url", "image_url": {"url": image_data_url(image_bytes)}},
            ]}],
            temperature=0,
            max_tokens=250,
            response_format={"type": "json_object"},
        )
        parsed = json.loads(response.choices[0].message.content or "{}")
        return {
            "is_pandal_image": bool(parsed.get("is_pandal_image")),
            "confidence": float(parsed.get("confidence", 0.0)),
            "reason": clean_text(parsed.get("reason"), 1000) or "",
        }
    except Exception as exc:
        return {"status": "error", "error": str(exc)[:1500]}


def persist_image(pandal_id: str, image_bytes: bytes, content_type: Optional[str]) -> Tuple[Path, str]:
    digest = sha256_bytes(image_bytes)
    IMAGE_DIR.mkdir(parents=True, exist_ok=True)
    ext = ".jpg"
    if content_type and "png" in content_type:
        ext = ".png"
    elif content_type and "webp" in content_type:
        ext = ".webp"
    elif content_type and "gif" in content_type:
        ext = ".gif"
    path = IMAGE_DIR / f"{pandal_id}_{digest[:16]}{ext}"
    if not path.exists():
        path.write_bytes(image_bytes)
    return path, digest


def validate_post_images(
    pandal_id: str,
    pandal_name: str,
    image_urls: List[str],
    post_datetime: datetime,
    post_url: Optional[str],
    cache: Dict[str, Any],
    vision_client: Optional[Any],
    vision_remaining: int,
) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]], int, bool, List[Dict[str, Any]]]:
    """Validate image candidates and report whether post-detail fallback is needed."""
    approved: List[Dict[str, Any]] = []
    pending: List[Dict[str, Any]] = []
    vision_decisions: List[Dict[str, Any]] = []
    used = 0
    needs_detail = False

    for image_url in image_urls[:MAX_IMAGE_URLS_FROM_DETAIL]:
        if is_facebook_url(image_url):
            needs_detail = True
            continue

        data, error, content_type = download_image(image_url)
        if data is None:
            # The timeline can return an apparent image URL that is actually a
            # Facebook HTML page, expired media URL, or an unusable thumbnail.
            # Trigger the selected-post detail endpoint so we can resolve the
            # real attachment URL.
            needs_detail = True
            pending.append({
                "url": image_url,
                "status": "pending",
                "post_datetime": post_datetime.isoformat(),
                "post_url": post_url,
                "reason": error,
                "last_checked_at": now_iso(),
            })
            continue

        path, digest = persist_image(pandal_id, data, content_type)
        cached = cache.get(digest)
        if cached and cached.get("decision") in {"approved", "rejected"} and "is_pandal_image" in cached:
            cached_confidence = float(cached.get("confidence", 0.0))
            cached_is_pandal = bool(cached.get("is_pandal_image"))
            cached_reason = cached.get("reason") or ""
            cached_approved = cached.get("decision") == "approved"
            vision_decisions.append({
                "source": "cached_vision",
                "is_pandal_image": cached_is_pandal,
                "confidence": cached_confidence,
                "reason": cached_reason,
                "approved": cached_approved,
            })
            if cached and cached.get("decision") == "approved":
                approved.append({
                    "url": image_url,
                    "local_file": str(path.relative_to(ROOT)),
                    "sha256": digest,
                    "source": "scrapebadger",
                    "post_datetime": post_datetime.isoformat(),
                    "post_url": post_url,
                    "validated_by": "cached_vision",
                    "confidence": cached.get("confidence"),
                    "reason": cached.get("reason"),
                    "approved_at": cached.get("approved_at") or now_iso(),
                })
                break

            if cached and cached.get("decision") == "rejected":
                continue

        if vision_client is None or used >= vision_remaining:
            pending.append({
                "url": image_url,
                "local_file": str(path.relative_to(ROOT)),
                "sha256": digest,
                "source": "scrapebadger",
                "status": "pending",
                "post_datetime": post_datetime.isoformat(),
                "post_url": post_url,
                "reason": "Vision validation unavailable or run cap reached",
                "last_checked_at": now_iso(),
            })
            continue

        result = validate_image_with_vision(
            vision_client,
            data,
            context=f"{pandal_name} Durga Puja {YEAR} official Facebook Page post",
        )
        used += 1

        if result.get("status") == "error":
            pending.append({
                "url": image_url,
                "local_file": str(path.relative_to(ROOT)),
                "sha256": digest,
                "source": "scrapebadger",
                "status": "pending",
                "post_datetime": post_datetime.isoformat(),
                "post_url": post_url,
                "reason": result.get("error"),
                "last_checked_at": now_iso(),
            })
            continue

        is_pandal = bool(result.get("is_pandal_image"))
        confidence = float(result.get("confidence", 0.0))
        reason = result.get("reason") or ""
        approved_decision = is_pandal and confidence >= VISION_CONFIDENCE_THRESHOLD
        vision_decisions.append({
            "source": VISION_MODEL,
            "is_pandal_image": is_pandal,
            "confidence": confidence,
            "reason": reason,
            "approved": approved_decision,
        })
        cache[digest] = {
            "decision": "approved" if approved_decision else "rejected",
            "is_pandal_image": is_pandal,
            "confidence": confidence,
            "reason": reason,
            "post_datetime": post_datetime.isoformat(),
            "validated_at": now_iso(),
            "approved_at": now_iso() if approved_decision else None,
        }

        if approved_decision:
            approved.append({
                "url": image_url,
                "local_file": str(path.relative_to(ROOT)),
                "sha256": digest,
                "source": "scrapebadger",
                "post_datetime": post_datetime.isoformat(),
                "post_url": post_url,
                "validated_by": VISION_MODEL,
                "confidence": confidence,
                "reason": reason,
                "approved_at": now_iso(),
            })
            break

    return approved[:MAX_IMAGES_PER_PANDAL], pending, used, needs_detail, vision_decisions

def extract_images_from_detail(detail: Dict[str, Any]) -> List[str]:
    """Extract post-owned attachment URLs from a ScrapeBadger single-post response.

    ScrapeBadger documents ``/posts/{post_id}`` as returning a single post with
    attachments, but the nested attachment schema is not fixed.  Inside the
    selected post's top-level ``attachments`` subtree we therefore accept ANY
    HTTP(S) URL and rank it instead of requiring the JSON key or URL itself to
    look like an image.  This handles CDN URLs stored under provider-specific
    keys such as ``uri``, ``href``, ``src_url``, etc.
    """
    ranked: List[Tuple[int, str]] = []
    seen: set[str] = set()

    def url_score(url: str, key_l: str, base_score: int) -> int:
        low = url.lower()
        score = base_score

        if "scontent" in low or "fbcdn" in low or "fbsbx" in low:
            score += 80
        if any(ext in low for ext in (".jpg", ".jpeg", ".png", ".webp", ".gif", ".avif")):
            score += 50
        if any(token in key_l for token in ("original", "full", "large", "image", "photo", "media")):
            score += 40
        if any(token in key_l for token in ("uri", "href", "src", "source", "url")):
            score += 20
        if any(token in key_l for token in ("thumbnail", "thumb", "small", "preview")):
            score -= 10
        if "photo.php" in low or "permalink" in low or "/posts/" in low:
            score -= 80
        if is_facebook_url(url):
            score -= 20
        return score

    def extract_embedded_urls(text: str) -> List[str]:
        # Attachment values are occasionally returned as JSON-ish strings or
        # strings containing one or more escaped URLs.
        urls = re.findall(r'https?://[^\s"\\<>]+', text)
        cleaned: List[str] = []
        for u in urls:
            u = u.rstrip("'`),]}")
            u = u.replace("\\/", "/")
            cleaned.append(u)
        return cleaned

    def add_url(url: str, key_l: str, base_score: int) -> None:
        url = (url or "").strip()
        if not url.startswith(("http://", "https://")):
            return
        if url in seen:
            return
        # Require an actual URL, but do NOT require it to look like an image.
        # The attachment subtree is already scoped to the selected post.
        seen.add(url)
        ranked.append((url_score(url, key_l, base_score), url))

    def walk(node: Any, base_score: int = 200, key_l: str = "attachments") -> None:
        if node is None:
            return

        if isinstance(node, str):
            for embedded in extract_embedded_urls(node):
                add_url(embedded, key_l, base_score)
            return

        if isinstance(node, dict):
            for key, value in node.items():
                key_text = str(key)
                key_norm = key_text.lower().replace("-", "_")
                child_score = base_score
                if any(token in key_norm for token in ("original", "full", "large", "image", "photo", "media", "source", "src", "uri", "href", "url")):
                    child_score += 30
                elif any(token in key_norm for token in ("thumbnail", "thumb", "small", "preview")):
                    child_score += 5
                if isinstance(value, str):
                    add_url(value, key_norm, child_score)
                    for embedded in extract_embedded_urls(value):
                        add_url(embedded, key_norm, child_score)
                else:
                    walk(value, child_score, key_norm)
            return

        if isinstance(node, list):
            for value in node:
                walk(value, base_score, key_l)

    attachments = detail.get("attachments") if isinstance(detail, dict) else None
    if attachments is not None:
        # If the provider ever serializes the attachment object as a JSON string,
        # decode it before walking it.
        if isinstance(attachments, str):
            parsed_attachments: Any = attachments
            try:
                parsed_attachments = json.loads(attachments)
            except Exception:
                pass
            walk(parsed_attachments, 250, "attachments")
        else:
            walk(attachments, 250, "attachments")

    # Fallback for provider versions that expose post media under a dedicated
    # top-level field. Never recurse through author/page metadata here.
    if not ranked and isinstance(detail, dict):
        allowed_top_level = {
            "media", "photos", "images", "image", "photo", "gallery",
            "media_urls", "image_urls", "photo_urls", "post_media",
            "post_images", "post_photos", "subattachments",
        }
        for key, value in detail.items():
            if str(key).lower() in allowed_top_level:
                walk(value, 150, str(key).lower())

    ranked.sort(key=lambda item: item[0], reverse=True)
    return [url for _, url in ranked[:MAX_IMAGE_URLS_FROM_DETAIL]]

def build_payload(records_by_id: Dict[str, Dict[str, Any]], freshness_days: int, vision_enabled: bool) -> Dict[str, Any]:
    return {
        "dataset_name": "Kolkata Pandal 2026 Dynamic Facebook Updates",
        "generated_year": YEAR,
        "source_policy": {
            "facebook_pages": "manual_verified_only",
            "anonymous_access_only": True,
            "login_required": False,
            "api_key_required_for_facebook_collection": True,
            "facebook_collection_provider": "ScrapeBadger",
            "storage_state_used": False,
            "previous_approved_data_preserved_on_failure": True,
            "strict_2026_post_filter": False,
            "freshness_window_days": freshness_days,
            "vision_validation": vision_enabled,
            "image_rule": "images must belong to the selected recent Facebook post",
            "collection_method": "scrapebadger_facebook_page_posts_plus_post_details",
        },
        "last_run": now_iso(),
        "records": [records_by_id[k] for k in sorted(records_by_id)],
    }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Collect verified Facebook Page posts via ScrapeBadger")
    parser.add_argument("--pandal-id", type=str, default=None)
    parser.add_argument("--limit", type=int, default=None)
    parser.add_argument("--freshness-days", type=int, default=FRESHNESS_DAYS)
    parser.add_argument("--max-vision-requests", type=int, default=MAX_VISION_REQUESTS_DEFAULT)
    parser.add_argument("--skip-vision", action="store_true")
    parser.add_argument("--debug-raw", action="store_true")
    parser.add_argument("--output", type=Path, default=OUTPUT_FILE)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    api_key = os.environ.get("SCRAPEBADGER_API_KEY")
    if not api_key:
        print("ERROR: SCRAPEBADGER_API_KEY is not set.")
        return 1

    verified, social_file, verified_count = load_verified_pages()
    print(f"Social mapping file: {social_file}")
    print(f"Social mapping exists: {social_file.exists()}")
    print(f"Verified Facebook records found: {verified_count}")
    print("Facebook login required: NO")
    print("Meta Graph API token required: NO")
    print("ScrapeBadger API key required: YES")
    print("Collection method: ScrapeBadger Facebook Page timeline + single-post detail for media")

    selected = sorted(verified)
    if args.pandal_id:
        selected = [args.pandal_id.strip().upper()] if args.pandal_id.strip().upper() in verified else []
        print(f"Requested pandal: {args.pandal_id.strip().upper()}")
    if args.limit is not None:
        selected = selected[: max(0, args.limit)]
    if not selected:
        print("No verified Facebook pages selected.")
        return 1

    vision_enabled = bool(GROQ_API_KEY) and not args.skip_vision
    vision_client = get_vision_client() if vision_enabled else None
    vision_remaining = max(0, args.max_vision_requests)
    previous = load_previous_records(args.output)
    cache = load_json(CACHE_FILE, {})
    if not isinstance(cache, dict):
        cache = {}

    print(f"Pages selected: {len(selected)}")
    print(f"Groq vision enabled: {bool(vision_client)}")

    records_by_id = dict(previous)
    successful = failed = no_recent = vision_total = detail_calls = web_media_calls = 0

    for index, pandal_id in enumerate(selected, start=1):
        source = verified[pandal_id]
        pandal_name = str(source.get("pandal_name") or source.get("name") or pandal_id)
        page_url = source["facebook_page"]
        identifier = extract_facebook_identifier(page_url)
        print(f"\n[{index}/{len(selected)}] {pandal_id} - {pandal_name}")
        print(f"  Facebook page: {page_url}")
        print(f"  ScrapeBadger identifier: {identifier}")

        current = base_record(pandal_id, source, max(1, args.freshness_days))
        current["last_scraped_at"] = now_iso()

        raw_page_file = RAW_DIR / f"{pandal_id}_page_posts.json" if args.debug_raw else None
        page_result = scrape_page_posts(identifier, debug_raw_file=raw_page_file)
        if page_result.get("status") != "success":
            failed += 1
            old = previous.get(pandal_id)
            if old:
                current = dict(old)
                current["scrape_status"] = "stale"
                current["last_scraped_at"] = now_iso()
                current["last_error"] = page_result.get("error")
            else:
                current["scrape_status"] = page_result.get("status") or "error"
                current["last_error"] = page_result.get("error")
            records_by_id[pandal_id] = current
            print(f"  status={current['scrape_status']} | error={current.get('last_error')}")
            continue

        normalized = [normalize_post(x) for x in page_result.get("posts") or []]
        dated = [x for x in normalized if x.get("post_datetime") is not None]
        dated.sort(key=lambda x: x["post_datetime"], reverse=True)
        if not dated:
            successful += 1
            no_recent += 1
            current["scrape_status"] = "success_no_dated_post"
            records_by_id[pandal_id] = current
            print(f"  status=success_no_dated_post | posts_returned={len(normalized)}")
            continue

        selected_post = dated[0]
        post_dt = selected_post["post_datetime"]
        cutoff = datetime.now(timezone.utc) - timedelta(days=max(1, args.freshness_days))
        if post_dt < cutoff:
            successful += 1
            no_recent += 1
            current["scrape_status"] = "success_no_recent_post"
            records_by_id[pandal_id] = current
            print(f"  status=success_no_recent_post | newest_post={post_dt.isoformat()} | window_days={args.freshness_days} | posts_returned={len(normalized)}")
            continue

        # Announcement/update comes directly from the selected timeline post.
        # This remains populated even when there is no usable image.
        latest_update = selected_post.get("post_text")
        post_url = selected_post.get("post_url")
        post_id = selected_post.get("id")

        # Prefer direct CDN media already exposed by the timeline response.
        candidate_urls = [
            u for u in selected_post.get("image_urls") or []
            if direct_image_candidate(u)
        ]

        approved, pending, used, needs_detail, vision_decisions = validate_post_images(
            pandal_id=pandal_id,
            pandal_name=pandal_name,
            image_urls=candidate_urls,
            post_datetime=post_dt,
            post_url=post_url,
            cache=cache,
            vision_client=vision_client,
            vision_remaining=vision_remaining,
        )
        vision_remaining -= used
        vision_total += used

        # A timeline candidate can exist but still be unusable (e.g. a
        # photo.php HTML page, expired CDN URL, or tiny thumbnail). Only then
        # resolve the selected post through ScrapeBadger's post-details API.
        if post_id and needs_detail:
            detail_file = RAW_DIR / f"{pandal_id}_post_{post_id}.json" if args.debug_raw else None
            detail_result = scrape_single_post(post_id, debug_file=detail_file)
            detail_calls += 1

            if detail_result.get("status") == "success":
                detail = detail_result.get("post") or {}
                detail_post = normalize_post(detail)
                if detail_post.get("post_text"):
                    latest_update = detail_post["post_text"]
                if detail_post.get("post_url"):
                    post_url = detail_post["post_url"]

                detail_urls = extract_images_from_detail(detail)
                if args.debug_raw:
                    print(f"  post_detail_media_candidates={len(detail_urls)}")
                    for media_url in detail_urls[:5]:
                        print(f"    media_candidate={media_url}")
                    if not detail_urls and isinstance(detail, dict):
                        top_keys = sorted(str(k) for k in detail.keys())
                        print(f"  post_detail_top_level_keys={top_keys}")
                        attachments = detail.get("attachments")
                        if attachments is not None:
                            try:
                                attachment_preview = json.dumps(attachments, ensure_ascii=False, indent=2)
                            except Exception:
                                attachment_preview = repr(attachments)
                            max_preview = 16000
                            if len(attachment_preview) > max_preview:
                                attachment_preview = attachment_preview[:max_preview] + "\n...<truncated>..."
                            print("  post_detail_attachments_preview=")
                            print(attachment_preview)
                        else:
                            print("  post_detail_attachments_preview=<missing>")
                        hints: List[str] = []

                        def collect_media_hints(node: Any, path: str = "detail") -> None:
                            if len(hints) >= 20:
                                return
                            if isinstance(node, dict):
                                for key, value in node.items():
                                    key_l = str(key).lower()
                                    child_path = f"{path}.{key}"
                                    if isinstance(value, str) and value.startswith(("http://", "https://")) and (
                                        "image" in key_l or "photo" in key_l or "media" in key_l or "thumb" in key_l
                                    ):
                                        hints.append(f"{child_path}={value}")
                                    elif isinstance(value, (dict, list)):
                                        collect_media_hints(value, child_path)
                            elif isinstance(node, list):
                                for idx, value in enumerate(node):
                                    collect_media_hints(value, f"{path}[{idx}]")

                        collect_media_hints(detail)
                        for hint in hints:
                            print(f"    media_hint={hint}")

                if not detail_urls and post_detail_has_url_less_video(detail):
                    # Native Facebook Video attachment has no URL/poster in the dedicated
                    # response. Try several public URL forms for the *same selected post/video*
                    # through ScrapeBadger's generic web scraper. Facebook sometimes returns
                    # a not_found result for one URL shape while another public shape resolves.
                    current.setdefault("media_resolution", {})["video_web_fallback"] = True

                    attachment_ids: List[str] = []
                    attachments_for_video = detail.get("attachments") if isinstance(detail, dict) else None

                    def collect_video_ids(node: Any) -> None:
                        if isinstance(node, dict):
                            type_text = str(node.get("type") or node.get("media_type") or "").lower()
                            if "video" in type_text:
                                value = node.get("id")
                                if value is not None:
                                    sid = str(value).strip()
                                    if sid and sid not in attachment_ids:
                                        attachment_ids.append(sid)
                            for value in node.values():
                                collect_video_ids(value)
                        elif isinstance(node, list):
                            for value in node:
                                collect_video_ids(value)

                    collect_video_ids(attachments_for_video)

                    web_targets: List[str] = []
                    seen_targets: set[str] = set()

                    def add_web_target(value: Optional[str]) -> None:
                        value = (value or "").strip()
                        if not value or not value.startswith(("http://", "https://")):
                            return
                        normalized = normalize_url(value)
                        if normalized and normalized not in seen_targets:
                            seen_targets.add(normalized)
                            web_targets.append(normalized)

                    # 1) Provider's permalink first.
                    add_web_target(post_url)

                    # 2) Canonical Page post URL reconstructed from our verified Page identifier.
                    if post_id:
                        fb_web_identifier = extract_facebook_identifier(str(page_url))
                        add_web_target(f"https://www.facebook.com/{fb_web_identifier}/posts/{post_id}")
                        add_web_target(f"https://facebook.com/{fb_web_identifier}/posts/{post_id}")

                    # 3) Native video URL shapes from the attachment id.
                    for vid in attachment_ids[:3]:
                        add_web_target(f"https://www.facebook.com/watch/?v={vid}")
                        add_web_target(f"https://www.facebook.com/video.php?v={vid}")
                        add_web_target(f"https://www.facebook.com/{fb_web_identifier}/videos/{vid}")

                    last_web_error: Optional[str] = None
                    for target_index, web_target in enumerate(web_targets[:6]):
                        web_file = (
                            RAW_DIR / f"{pandal_id}_post_web_media_{target_index}.json"
                            if args.debug_raw else None
                        )
                        web_media_calls += 1
                        web_result = scrapebadger_web_scrape(web_target, debug_file=web_file)
                        if web_result.get("status") == "success":
                            web_urls = extract_image_urls_from_html(web_result.get("content") or "")
                            if web_urls:
                                detail_urls = web_urls
                                current.setdefault("media_resolution", {})["video_web_source_url"] = web_target
                                print(f"  video_post_web_media_candidates={len(web_urls)}")
                                print(f"  video_post_web_source_url={web_target}")
                                for media_url in web_urls[:5]:
                                    print(f"    web_media_candidate={media_url}")
                                break
                        else:
                            last_web_error = web_result.get("error")
                            print(f"  video_post_web_media_attempt={target_index + 1} | url={web_target} | error={last_web_error}")

                    if not detail_urls:
                        current.setdefault("media_resolution", {})["web_scrape_error"] = last_web_error or "No video poster/image found"
                        print(f"  video_post_web_media_candidates=0")

                if detail_urls:
                    approved2, pending2, used2, _, detail_vision_decisions = validate_post_images(
                        pandal_id=pandal_id,
                        pandal_name=pandal_name,
                        image_urls=detail_urls,
                        post_datetime=post_dt,
                        post_url=post_url,
                        cache=cache,
                        vision_client=vision_client,
                        vision_remaining=vision_remaining,
                    )
                    vision_remaining -= used2
                    vision_total += used2

                    # The post-detail response is the authoritative fallback
                    # for media. Prefer it whenever it provides candidates.
                    approved = approved2
                    pending = pending2
                    vision_decisions.extend(detail_vision_decisions)
            else:
                print(f"  media_detail_error={detail_result.get('error')}")

        current["latest_update"] = latest_update
        current["latest_announcement"] = latest_update
        current["latest_post_date"] = post_dt.isoformat()
        current["latest_post_url"] = post_url
        current["images"] = approved
        current["pending_images"] = pending
        current["scrape_status"] = "success"
        records_by_id[pandal_id] = current
        successful += 1

        for decision in vision_decisions:
            source = decision.get("source") or VISION_MODEL
            is_pandal = bool(decision.get("is_pandal_image"))
            confidence = float(decision.get("confidence", 0.0))
            approved_decision = bool(decision.get("approved"))
            reason = decision.get("reason") or ""
            print(f"  Vision result ({source}):")
            print(f"    is_pandal_image: {str(is_pandal).lower()}")
            print(f"    confidence: {confidence:.2f}")
            print(f"    approved: {str(approved_decision).upper()} (threshold={VISION_CONFIDENCE_THRESHOLD:.2f})")
            print(f"    reason: {reason or 'No reason returned'}")

        print(
            f"  status=success | recent_post={post_dt.isoformat()} | "
            f"images_found={len(candidate_urls)} | approved_images={len(approved)} | pending={len(pending)} | "
            f"post_detail_calls_total={detail_calls}"
        )

        atomic_write(args.output, build_payload(records_by_id, args.freshness_days, bool(vision_client)))
        atomic_write(CACHE_FILE, cache)

    if records_by_id:
        atomic_write(args.output, build_payload(records_by_id, args.freshness_days, bool(vision_client)))
    else:
        print("No records were successfully collected; existing output was left untouched.")
    atomic_write(CACHE_FILE, cache)

    print("\nCompleted")
    print(f"  Successful pages       : {successful}")
    print(f"  Pages with no recent post: {no_recent}")
    print(f"  API failures           : {failed}")
    print(f"  Single-post detail calls: {detail_calls}")
    print(f"  Video-page media fallbacks: {web_media_calls}")
    print(f"  Vision calls            : {vision_total}")
    print(f"  Output                  : {args.output}")
    return 0 if successful or no_recent else 2


if __name__ == "__main__":
    raise SystemExit(main())
