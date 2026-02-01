#!/usr/bin/env python3
"""
Scrape all games from NVIDIA GeForce NOW via their GraphQL API and output
them in Google Play Game Actions DataFeed format (schema.org JSON-LD).

Reference:
  - GraphQL endpoint: https://games.geforce.com/graphql
  - Google Play Game Actions: https://developers.google.com/actions/media/play-game-actions
"""

import argparse
import json
import sys
import time
from datetime import datetime, timezone

import requests

GRAPHQL_URL = "https://games.geforce.com/graphql"
PAGE_SIZE = 1200
MAX_RETRIES = 4
INITIAL_BACKOFF = 2  # seconds


# ---------------------------------------------------------------------------
# GraphQL helpers
# ---------------------------------------------------------------------------

def build_query(cursor: str = "", page_size: int = PAGE_SIZE) -> str:
    """Return the full GraphQL query string for fetching GFN apps."""
    return (
        "{"
        f'apps(first: {page_size}, after: "{cursor}") {{'
        "  numberReturned"
        "  pageInfo { hasNextPage endCursor totalCount }"
        "  items {"
        "    osType"
        "    id"
        "    cmsId"
        "    sortName"
        "    title"
        "    longDescription"
        "    contentRatings { type categoryKey }"
        "    developerName"
        "    geForceUrl"
        "    images {"
        "      FEATURE_IMAGE"
        "      GAME_BOX_ART"
        "      HERO_IMAGE"
        "      MARQUEE_HERO_IMAGE"
        "      KEY_ART"
        "      KEY_ICON"
        "      KEY_IMAGE"
        "      TV_BANNER"
        "      SCREENSHOTS"
        "      SCREENSHOT_THUMB"
        "    }"
        "    keywords"
        "    maxLocalPlayers"
        "    maxOnlinePlayers"
        "    publisherName"
        "    storeIds { id store }"
        "    streamingModes { framesPerSecond heightInPixels widthInPixels }"
        "    supportedControls"
        "    supportedGamePlayModes"
        "    type"
        "    computedValues { earliestReleaseDate earliestStreetDate allKeywords }"
        "    genres"
        "    appStore"
        "    variants {"
        "      id title appStore developerName"
        "      gfn { status visibility releaseDate isInLibrary }"
        "      osType storeId"
        "    }"
        "  }"
        "} }"
    )


def fetch_page(cursor: str = "", session: requests.Session | None = None) -> tuple[list[dict], dict]:
    """Fetch a single page of results from the GFN GraphQL API.

    Returns (items, page_info).
    """
    s = session or requests.Session()
    query = build_query(cursor)

    for attempt in range(MAX_RETRIES):
        try:
            resp = s.post(GRAPHQL_URL, json={"query": query}, timeout=30)
            resp.raise_for_status()
            data = resp.json()
            apps = data["data"]["apps"]
            return apps["items"], apps["pageInfo"]
        except (requests.RequestException, KeyError, json.JSONDecodeError) as exc:
            wait = INITIAL_BACKOFF * (2 ** attempt)
            print(
                f"  [retry {attempt + 1}/{MAX_RETRIES}] {exc!r} — waiting {wait}s",
                file=sys.stderr,
            )
            time.sleep(wait)

    print("ERROR: Failed to fetch page after retries. Stopping.", file=sys.stderr)
    sys.exit(1)


def fetch_all_games(session: requests.Session | None = None) -> list[dict]:
    """Paginate through the GFN GraphQL API and return every game."""
    all_items: list[dict] = []
    cursor = ""
    page = 0

    while True:
        page += 1
        print(f"Fetching page {page} (cursor={cursor!r}) …", file=sys.stderr)
        items, page_info = fetch_page(cursor, session=session)
        all_items.extend(items)
        print(
            f"  got {len(items)} items (total so far: {len(all_items)}"
            f" / {page_info.get('totalCount', '?')})",
            file=sys.stderr,
        )

        if not page_info.get("hasNextPage"):
            break
        cursor = page_info["endCursor"]

    return all_items


# ---------------------------------------------------------------------------
# Conversion to Google Play Game Actions DataFeed (schema.org JSON-LD)
# ---------------------------------------------------------------------------

def pick_image(images: dict | None) -> str | None:
    """Pick the best available image URL from the images dict."""
    if not images:
        return None
    for key in ("GAME_BOX_ART", "KEY_ART", "KEY_IMAGE", "HERO_IMAGE",
                "FEATURE_IMAGE", "MARQUEE_HERO_IMAGE", "TV_BANNER"):
        val = images.get(key)
        if val:
            return val
    return None


def format_content_rating(content_ratings: list[dict] | None) -> str | None:
    """Convert GFN contentRatings list into a human-readable string.

    GFN returns objects like:
        {"type": "ESRB", "categoryKey": "TEEN"}
    We convert to e.g. "ESRB T" or "ESRB E10+".
    """
    if not content_ratings:
        return None

    # Prefer ESRB, then PEGI, then first available
    esrb = None
    pegi = None
    first = None
    for cr in content_ratings:
        rating_type = (cr.get("type") or "").upper()
        category = cr.get("categoryKey") or ""
        label = f"{rating_type} {category}".strip()
        if not first:
            first = label
        if rating_type == "ESRB":
            esrb = label
        elif rating_type == "PEGI":
            pegi = label

    return esrb or pegi or first


def build_geforce_play_url(gfn_item: dict) -> str:
    """Build a GeForce NOW deep-link URL for the game."""
    cmsId = gfn_item.get("cmsId") or gfn_item.get("id") or ""
    return f"https://play.geforcenow.com/mall/#/deeplink?game-id={cmsId}"


def gfn_item_to_videogame(item: dict) -> dict:
    """Convert a single GFN API item to a schema.org VideoGame (Work) entity."""
    play_url = build_geforce_play_url(item)
    image_url = pick_image(item.get("images"))
    content_rating = format_content_rating(item.get("contentRatings"))

    # --- VideoGame (Work) ---
    vg: dict = {
        "@context": "http://schema.org",
        "@type": "VideoGame",
        "@id": f"gfn-{item.get('id', '')}",
        "name": item.get("title", ""),
        "url": play_url,
        "applicationCategory": "Game",
    }

    description = item.get("longDescription")
    if description:
        vg["description"] = description

    genres = item.get("genres")
    if genres:
        vg["genre"] = genres

    if content_rating:
        vg["contentRating"] = content_rating

    if image_url:
        vg["image"] = image_url

    publisher = item.get("publisherName")
    if publisher:
        vg["publisher"] = {
            "@type": "Organization",
            "name": publisher,
        }

    developer = item.get("developerName")
    if developer:
        vg["contributor"] = {
            "@type": "Organization",
            "name": developer,
            "roleName": "developer",
        }

    keywords = item.get("keywords")
    if keywords:
        vg["keywords"] = keywords if isinstance(keywords, list) else [keywords]

    # gamePlatform from supportedControls / osType
    platforms = []
    if item.get("osType"):
        platforms.append(item["osType"])
    if platforms:
        vg["gamePlatform"] = platforms

    # numberOfPlayers hints
    max_local = item.get("maxLocalPlayers")
    max_online = item.get("maxOnlinePlayers")
    if max_local:
        vg["numberOfPlayers"] = max_local
    if max_online:
        vg["numberOfPlayers"] = max_online

    play_modes = item.get("supportedGamePlayModes")
    if play_modes:
        vg["playMode"] = play_modes

    # Store links
    store_ids = item.get("storeIds") or []
    same_as: list[str] = []
    for sid in store_ids:
        store = (sid.get("store") or "").upper()
        store_id = sid.get("id") or ""
        if store == "STEAM" and store_id:
            same_as.append(f"https://store.steampowered.com/app/{store_id}")
        elif store == "EPIC" and store_id:
            same_as.append(f"https://store.epicgames.com/p/{store_id}")
    if same_as:
        vg["sameAs"] = same_as

    # Release date
    computed = item.get("computedValues") or {}
    release_date = computed.get("earliestReleaseDate") or computed.get("earliestStreetDate")
    if release_date:
        vg["datePublished"] = release_date

    # --- VideoGame (Edition) via exampleOfWork ---
    edition: dict = {
        "@context": "http://schema.org",
        "@type": "VideoGame",
        "name": f"{item.get('title', '')} (GeForce NOW)",
        "gamePlatform": ["PC"],
        "potentialAction": {
            "@type": "PlayGameAction",
            "target": {
                "@type": "EntryPoint",
                "urlTemplate": play_url,
                "actionPlatform": [
                    "http://schema.org/DesktopWebPlatform",
                ],
            },
        },
    }

    # Add store-specific editions from variants
    editions = [edition]
    for variant in (item.get("variants") or []):
        v_store = variant.get("appStore") or ""
        v_title = variant.get("title") or item.get("title", "")
        gfn_info = variant.get("gfn") or {}
        if gfn_info.get("status") not in (None, "AVAILABLE", "MAINTENANCE"):
            continue
        var_edition = {
            "@context": "http://schema.org",
            "@type": "VideoGame",
            "name": f"{v_title} ({v_store})" if v_store else v_title,
            "gamePlatform": ["PC"],
        }
        if v_store:
            var_edition["applicationCategory"] = v_store
        editions.append(var_edition)

    vg["exampleOfWork"] = editions
    return vg


def build_datafeed(games: list[dict]) -> dict:
    """Wrap a list of schema.org VideoGame entities into a DataFeed envelope."""
    return {
        "@context": "http://schema.org",
        "@type": "DataFeed",
        "dateModified": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "dataFeedElement": games,
    }


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(
        description="Scrape NVIDIA GeForce NOW games and output Google Play Game Actions DataFeed JSON."
    )
    parser.add_argument(
        "-o", "--output",
        default=None,
        help="Output file path (default: stdout)",
    )
    parser.add_argument(
        "--raw",
        action="store_true",
        help="Also save raw GFN API response to <output>.raw.json",
    )
    parser.add_argument(
        "--indent",
        type=int,
        default=2,
        help="JSON indentation level (default: 2)",
    )
    args = parser.parse_args()

    print("Starting GeForce NOW game scrape …", file=sys.stderr)
    session = requests.Session()
    session.headers.update({
        "Content-Type": "application/json",
        "Accept": "application/json",
        "User-Agent": "GFN-Scraper/1.0",
    })

    raw_games = fetch_all_games(session=session)
    print(f"\nFetched {len(raw_games)} total items from GeForce NOW.", file=sys.stderr)

    # Optionally dump raw data
    if args.raw and args.output:
        raw_path = args.output + ".raw.json"
        with open(raw_path, "w", encoding="utf-8") as f:
            json.dump(raw_games, f, indent=args.indent, ensure_ascii=False)
        print(f"Raw API data saved to {raw_path}", file=sys.stderr)

    # Convert to Google Play Game Actions DataFeed format
    videogame_entities = [gfn_item_to_videogame(item) for item in raw_games]
    datafeed = build_datafeed(videogame_entities)

    output_json = json.dumps(datafeed, indent=args.indent, ensure_ascii=False)

    if args.output:
        with open(args.output, "w", encoding="utf-8") as f:
            f.write(output_json)
            f.write("\n")
        print(f"DataFeed JSON saved to {args.output}", file=sys.stderr)
    else:
        print(output_json)

    print("Done.", file=sys.stderr)


if __name__ == "__main__":
    main()
