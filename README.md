# Kolkata Puja Tourist MCP

A Model Context Protocol (MCP) server for exploring Kolkata Durga Puja pandals and planning local visits using the project's 2026 datasets. It exposes tools for pandal discovery, walking/driving routes, date-specific Metro route suggestions, nearby restaurants, and selected traffic/condition context.


## Features

- Search pandals by name, area, address, or zone.
- Look up pandal details and find nearby pandals.
- Calculate driving and pedestrian routes through Geoapify.
- Plan multi-stop driving routes, including an optional optimized order.
- Build a mixed-mode Puja itinerary with walking and a date-specific Metro station route where the planner's access rules allow it.
- Find nearest Metro stations and inspect Kolkata Metro's recorded special Puja service information.
- Search the local restaurant dataset and inspect recorded opening-hour information.
- Retrieve curated Puja routes, ritual schedule data, selected verified pandal social-page links, traffic advisories, and road-traffic-derived area-busyness snapshots.

The server uses locally bundled datasets for many lookups. External route calculation requires a Geoapify API key. Data sources and caveats are recorded in relevant dataset files and tool responses where available.

## Important limitations


- **Metro suggestions are not live train tracking.** Published service data can change; check official Metro Railway notices for amendments or disruptions.
- **Busyness is a proxy.** The busyness snapshots are derived from sampled nearby road-traffic conditions. They are not direct pedestrian counts, queues, passenger density, or crowd forecasts.
- **Restaurant information is from the local dataset.** It is not live reservation availability, and listed opening hours may change.

## Tools and resources

### Pandal discovery

- `search_pandals`
- `get_pandal_details`
- `find_nearby_pandals`
- `find_nearby_pandals_by_name`
- `get_pandal_crowding`
- `get_latest_pandal_update`
- `get_nearby_pandal_official_pages`

### Route and itinerary planning

- `get_route_between_pandals`
- `plan_puja_route`
- `plan_walking_puja_route`
- `recommend_puja_route`
- `get_planned_puja_routes`
- `plan_puja_trip`

### Metro

- `get_nearest_metro`
- `get_nearest_metro_by_pandal`
- `get_metro_station_crowding`
- `plan_metro_journey`
- `get_metro_puja_special_services`
- `get_routing_service_health`

### Restaurants and conditions

- `search_restaurants`
- `find_nearby_restaurants`
- `find_nearby_restaurants_by_pandal`
- `get_restaurant_availability`
- `get_live_traffic`
- `get_puja_schedule`

### MCP resources

- `puja://2026/pandals`
- `puja://transport`
- `puja://restaurants`
- `puja://2026/schedule`
- `metro://2026/puja-special-services`

Tool names and input schemas exposed by the running MCP server are authoritative. Use your MCP client or MCP Inspector to inspect the current schemas.

## Requirements

- Python 3.10 or later (use a Python version supported by the dependencies pinned in this repository).
- The project dependency file (`requirements.txt`) and the data files referenced by `src/server.py`.
- A Geoapify API key for driving/walking route calculations.
- Node.js/npm only if you want to run MCP Inspector via `npx`.

## Local setup

Run these commands from the project root.

### Windows PowerShell

```powershell
py -m venv .venv
.\.venv\Scripts\Activate.ps1
python -m pip install --upgrade pip
python -m pip install -r requirements.txt
```

Set the API key in the current PowerShell session without putting it in a source file:

```powershell
$env:GEOAPIFY_API_KEY = Read-Host "Enter Geoapify API key"
$env:MCP_TRANSPORT = "streamable-http"
$env:PORT = "8000"
python src/server.py
```

Keep the terminal running while using the server. The key is read by the server from the environment and must never be committed to Git.

### macOS / Linux

```bash
python3 -m venv .venv
source .venv/bin/activate
python -m pip install --upgrade pip
python -m pip install -r requirements.txt

export GEOAPIFY_API_KEY='YOUR_GEOAPIFY_API_KEY'
export MCP_TRANSPORT='streamable-http'
export PORT='8000'
python src/server.py
```

Do not replace the placeholder in a README or commit a real key. Use environment variables or your hosting provider's secret settings.

## Local endpoints

With the HTTP server running:

- MCP endpoint: `http://127.0.0.1:8000/mcp`
- Liveness endpoint: `http://127.0.0.1:8000/healthz`

Check liveness in another PowerShell window:

```powershell
Invoke-RestMethod http://127.0.0.1:8000/healthz
```

Expected response:

```json
{"status":"ok"}
```

On macOS/Linux, use `curl http://127.0.0.1:8000/healthz`.

### Test with MCP Inspector

With the server running, open another terminal and run:

```bash
npx -y @modelcontextprotocol/inspector --server-url http://127.0.0.1:8000/mcp --transport http
```

Use Inspector to list tools/resources and invoke a local lookup before trying an external route call.

## Run tests

From the project root:

```powershell
.\.venv\Scripts\python.exe -m py_compile src\server.py
.\.venv\Scripts\python.exe -m unittest discover -s tests -v
```

On macOS/Linux, use `python -m py_compile src/server.py` and `python -m unittest discover -s tests -v` from the activated virtual environment.

Routing integration tests may consume external API credits. Prefer mocked/offline tests for routine regression checks. A passing unit-test suite is not a substitute for load testing.

## Configuration

The following are the main optional environment settings. Values shown are the code defaults; review `src/server.py` for the full list and bounds.

| Variable | Default | Purpose |
|---|---:|---|
| `GEOAPIFY_API_KEY` | unset | Required for Geoapify route calculations. Keep secret. |
| `GEOAPIFY_BASE_URL` | `https://api.geoapify.com/v1` | Geoapify API base URL. |
| `MCP_TRANSPORT` | `streamable-http` | Use `streamable-http` for remote HTTP access or `stdio` for a local desktop-client process. |
| `PORT` | `8000` | HTTP listen port; hosting platforms commonly supply this. |
| `MCP_ALLOWED_HOSTS` | empty | Additional comma-separated HTTP host allowlist entries. |
| `MCP_ALLOWED_ORIGINS` | empty | Additional comma-separated origin allowlist entries. |
| `MCP_PER_CLIENT_RATE_LIMIT_PER_MINUTE` | `100` | Per-client, per-process request limit. |
| `MCP_GLOBAL_RATE_LIMIT_PER_MINUTE` | `1000` | Global, per-process request limit. |
| `MCP_MAX_CONCURRENT_REQUESTS` | `100` | Maximum active MCP HTTP requests per process. |
| `MCP_MAX_SESSIONS` | `100` | Maximum MCP sessions per process. |
| `MCP_SESSION_IDLE_TIMEOUT_SECONDS` | `600` | MCP session idle timeout. |
| `MCP_MAX_REQUEST_BODY_BYTES` | `1000000` | Maximum HTTP request body size. |
| `OUTBOUND_HTTP_MAX_CONCURRENCY` | `20` | Maximum concurrent outbound HTTP operations per process. |
| `GEOAPIFY_DAILY_CREDIT_BUDGET` | `2000` | Local estimated per-process routing budget; not the provider's authoritative quota counter. |
| `MAX_ROUTE_STOPS` | `25` | Maximum stops accepted by route tools. |
| `MAX_OPTIMIZED_ROUTE_STOPS` | `12` | Default upper bound for optimized multi-stop routes. |
| `ROUTE_CACHE_TTL_SECONDS` | `300` | In-process route cache lifetime. |
| `ROUTE_CACHE_MAX_ENTRIES` | `512` | Maximum in-process route cache entries. |
| `WEATHER_CACHE_TTL_SECONDS` | `600` | In-process weather cache lifetime. |

The server is intentionally usable without a shared `MCP_API_TOKEN`. That makes the endpoint public to clients that can reach it; rate limits and host/origin validation do not replace authentication or an edge abuse-protection layer.

## Deploy to Render (public demo)

1. Push the tested project to GitHub. Ensure `requirements.txt`, the `src/` code, tests, and all required `data/` files are included.
2. Create a Render Web Service connected to the repository.
3. Use a build command such as `pip install -r requirements.txt`.
4. Use the start command `python src/server.py`.
5. Add `GEOAPIFY_API_KEY` in Render's private Environment settings. Set `MCP_TRANSPORT=streamable-http`. Let Render provide `PORT`.
6. After deployment, verify `https://YOUR-SERVICE.onrender.com/healthz` and connect your MCP client to `https://YOUR-SERVICE.onrender.com/mcp`.
7. If using a custom domain, configure `MCP_ALLOWED_HOSTS` and `MCP_ALLOWED_ORIGINS` appropriately.

Do not put secrets into the GitHub repository, README, Docker image, or client-side configuration. Rotate any key that was exposed.

## Security and scaling status

This implementation includes host/origin validation, DNS-rebinding protection, request-body limits, per-process request/concurrency limits, a bounded outbound-request semaphore, in-process caches, and an estimated in-process Geoapify usage budget.

Those controls are **per process**. They are not shared across multiple workers or instances, and in-memory counters reset on process restart. Dynamic JSON snapshots are also read from the local filesystem; separate instances do not automatically share updates. Public multi-instance production should add a shared cache/rate-limit/usage store, reliable shared storage for data that must persist, trusted proxy/IP handling, monitoring, and load testing.


## Data and usage notes

- The project focuses on Kolkata Durga Puja 2026 and includes year-specific files. Refresh and verify date-sensitive data before using it for a later festival season.
- Straight-line distance is explicitly distinguished from walking or driving distance in the tool responses.
- Metro special-service notices and local opening-hour data can change; verify current official notices before travel.
- Road-traffic-derived busyness data should not be presented as a pedestrian crowd or queue measurement.
- Respect attribution and license requirements for the sources identified in the bundled datasets (including OpenStreetMap data where used).
