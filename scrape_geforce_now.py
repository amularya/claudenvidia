#!/usr/bin/env python3
"""
Scrape all games from NVIDIA GeForce NOW via their GraphQL API and output
them in Google Play Game Actions DataFeed format (schema.org JSON-LD).

Strategy:
  1. Run a GraphQL introspection query to discover the current schema.
  2. Build the richest possible query from discovered fields.
  3. Paginate through all results.
  4. If GraphQL fails entirely, fall back to the static JSON endpoint.

Reference:
  - GraphQL endpoint: https://games.geforce.com/graphql
  - Static JSON fallback: https://static.nvidiagrid.net/supported-public-game-list/gfnpc.json
  - Google Play Game Actions: https://developers.google.com/actions/media/play-game-actions
"""

import argparse
import json
import sys
import time
from datetime import datetime, timezone
from typing import Any, Optional

import requests

GRAPHQL_URL = "https://games.geforce.com/graphql"
STATIC_JSON_URL = "https://static.nvidiagrid.net/supported-public-game-list/gfnpc.json"
PAGE_SIZE = 1200
MAX_RETRIES = 3
INITIAL_BACKOFF = 2  # seconds


# ---------------------------------------------------------------------------
# Low-level HTTP helpers
# ---------------------------------------------------------------------------

def _post_graphql(session: requests.Session, query: str) -> dict:
    """POST a GraphQL query and return the parsed JSON response.

    Raises on HTTP errors or JSON parse failures (no retry logic here).
    """
    resp = session.post(GRAPHQL_URL, json={"query": query}, timeout=30)
    if not resp.ok:
        body = resp.text[:500]
        raise requests.HTTPError(
            f"{resp.status_code} {resp.reason} — body: {body}",
            response=resp,
        )
    data = resp.json()
    if "errors" in data:
        raise ValueError(f"GraphQL errors: {json.dumps(data['errors'][:3], indent=2)}")
    return data


def _post_graphql_with_retry(session: requests.Session, query: str) -> dict:
    """POST with exponential-backoff retry for transient failures."""
    last_exc = None
    for attempt in range(MAX_RETRIES):
        try:
            return _post_graphql(session, query)
        except Exception as exc:
            last_exc = exc
            wait = INITIAL_BACKOFF * (2 ** attempt)
            print(f"  [attempt {attempt + 1}/{MAX_RETRIES}] {exc} — retrying in {wait}s",
                  file=sys.stderr)
            time.sleep(wait)
    raise last_exc  # type: ignore[misc]


# ---------------------------------------------------------------------------
# Schema introspection
# ---------------------------------------------------------------------------

INTROSPECTION_QUERY = """
{
  __schema {
    queryType { name }
    types {
      name
      kind
      fields {
        name
        type {
          name
          kind
          ofType { name kind ofType { name kind ofType { name kind } } }
        }
      }
    }
  }
}
"""


def introspect_schema(session: requests.Session) -> dict[str, Any]:
    """Run an introspection query and return a map of type-name -> fields."""
    print("Running schema introspection …", file=sys.stderr)
    data = _post_graphql(session, INTROSPECTION_QUERY)
    types_map: dict[str, Any] = {}
    for t in data["data"]["__schema"]["types"]:
        if t.get("fields"):
            types_map[t["name"]] = {f["name"]: f["type"] for f in t["fields"]}
    return types_map


def discover_apps_query_fields(types_map: dict[str, Any]) -> Optional[str]:
    """Use introspected schema to build the richest possible apps query.

    Returns None if introspection didn't find the expected types.
    """
    # Find the root query type's 'apps' field
    query_type = types_map.get("Query") or types_map.get("Root") or {}
    if "apps" not in query_type:
        return None

    # Resolve the return type of 'apps'
    apps_type_info = query_type["apps"]
    apps_type_name = _resolve_type_name(apps_type_info)
    apps_fields = types_map.get(apps_type_name, {})
    if not apps_fields:
        return None

    # Build a selection set for the items type
    items_type_name = None
    if "items" in apps_fields:
        items_type_name = _resolve_type_name(apps_fields["items"])

    items_fields = types_map.get(items_type_name, {}) if items_type_name else {}
    items_selection = _build_selection(items_fields, types_map, depth=2)

    # Build pageInfo selection
    page_info_fields = types_map.get("PageInfo", {})
    page_info_selection = " ".join(page_info_fields.keys()) if page_info_fields else "hasNextPage endCursor totalCount"

    return (
        f"numberReturned\n"
        f"    pageInfo {{ {page_info_selection} }}\n"
        f"    items {{\n{items_selection}\n    }}"
    )


def _resolve_type_name(type_info: dict) -> str:
    """Walk through NonNull/List wrappers to find the actual type name."""
    t = type_info
    while t:
        if t.get("name") and t["kind"] not in ("NON_NULL", "LIST"):
            return t["name"]
        t = t.get("ofType", {})
    return ""


def _build_selection(fields: dict, types_map: dict, depth: int) -> str:
    """Recursively build a GraphQL selection set from known fields."""
    if depth <= 0 or not fields:
        return " ".join(fields.keys())

    lines = []
    for fname, ftype in fields.items():
        resolved = _resolve_type_name(ftype)
        sub_fields = types_map.get(resolved, {})
        # Skip __-prefixed introspection types
        if fname.startswith("__"):
            continue
        if sub_fields and resolved not in ("String", "Int", "Float", "Boolean", "ID"):
            sub_sel = _build_selection(sub_fields, types_map, depth - 1)
            lines.append(f"      {fname} {{ {sub_sel} }}")
        else:
            lines.append(f"      {fname}")
    return "\n".join(lines)


# ---------------------------------------------------------------------------
# GraphQL fetching
# ---------------------------------------------------------------------------

# Known-good field sets to try, from richest to minimal.
# We try each until one succeeds.
QUERY_TEMPLATES = [
    # Template 0: full fields (from 2024 schema)
    """{
  apps(first: %(limit)d, after: "%(cursor)s") {
    numberReturned
    pageInfo { hasNextPage endCursor totalCount }
    items {
      osType
      id
      cmsId
      sortName
      title
      longDescription
      contentRatings { type categoryKey }
      developerName
      geForceUrl
      images { FEATURE_IMAGE GAME_BOX_ART HERO_IMAGE KEY_ART KEY_ICON KEY_IMAGE TV_BANNER SCREENSHOTS }
      keywords
      maxLocalPlayers
      maxOnlinePlayers
      publisherName
      storeIds { id store }
      streamingModes { framesPerSecond heightInPixels widthInPixels }
      supportedControls
      supportedGamePlayModes
      type
      computedValues { earliestReleaseDate earliestStreetDate allKeywords }
      genres
      appStore
      variants { id title appStore developerName gfn { status visibility releaseDate isInLibrary } osType storeId }
    }
  }
}""",
    # Template 1: without images sub-fields (images might have changed)
    """{
  apps(first: %(limit)d, after: "%(cursor)s") {
    numberReturned
    pageInfo { hasNextPage endCursor totalCount }
    items {
      osType
      id
      cmsId
      sortName
      title
      longDescription
      contentRatings { type categoryKey }
      developerName
      publisherName
      storeIds { id store }
      supportedControls
      type
      computedValues { earliestReleaseDate earliestStreetDate }
      genres
      appStore
      variants { id title appStore developerName gfn { status visibility releaseDate isInLibrary } osType storeId }
    }
  }
}""",
    # Template 2: minimal safe fields
    """{
  apps(first: %(limit)d, after: "%(cursor)s") {
    numberReturned
    pageInfo { hasNextPage endCursor totalCount }
    items {
      id
      title
      sortName
      publisherName
      developerName
      genres
      type
      appStore
    }
  }
}""",
]


def probe_working_template(session: requests.Session) -> Optional[int]:
    """Try each query template with a small page size to find one that works."""
    print("Probing API to find a working query template …", file=sys.stderr)
    for idx, template in enumerate(QUERY_TEMPLATES):
        query = template % {"limit": 1, "cursor": ""}
        try:
            data = _post_graphql(session, query)
            items = data.get("data", {}).get("apps", {}).get("items", [])
            print(f"  Template {idx} works! (got {len(items)} test item(s))", file=sys.stderr)
            return idx
        except Exception as exc:
            print(f"  Template {idx} failed: {exc}", file=sys.stderr)
    return None


def try_introspected_query(session: requests.Session) -> Optional[str]:
    """Attempt introspection and return a custom query body, or None."""
    try:
        types_map = introspect_schema(session)
        fields = discover_apps_query_fields(types_map)
        if fields:
            # Test it
            test_query = '{ apps(first: 1, after: "") { %s } }' % fields
            data = _post_graphql(session, test_query)
            items = data.get("data", {}).get("apps", {}).get("items", [])
            if items is not None:
                print(f"  Introspected query works! ({len(items)} test item(s))", file=sys.stderr)
                return fields
    except Exception as exc:
        print(f"  Introspection failed: {exc}", file=sys.stderr)
    return None


def fetch_all_graphql(session: requests.Session) -> Optional[list[dict]]:
    """Try to fetch all games via GraphQL. Returns None if all approaches fail."""

    # Step 1: try introspection-based query
    introspected_fields = try_introspected_query(session)

    # Step 2: determine which query to use
    query_str = None
    if introspected_fields:
        query_str = '{{ apps(first: %(limit)d, after: "%(cursor)s") {{ {fields} }} }}'.format(
            fields=introspected_fields
        )
        print("Using introspection-derived query.", file=sys.stderr)
    else:
        template_idx = probe_working_template(session)
        if template_idx is not None:
            query_str = QUERY_TEMPLATES[template_idx]
            print(f"Using predefined template {template_idx}.", file=sys.stderr)

    if query_str is None:
        print("All GraphQL query approaches failed.", file=sys.stderr)
        return None

    # Step 3: paginate
    all_items: list[dict] = []
    cursor = ""
    page = 0

    while True:
        page += 1
        query = query_str % {"limit": PAGE_SIZE, "cursor": cursor}
        print(f"Fetching page {page} …", file=sys.stderr)

        try:
            data = _post_graphql_with_retry(session, query)
        except Exception as exc:
            print(f"ERROR on page {page}: {exc}", file=sys.stderr)
            if all_items:
                print(f"Returning {len(all_items)} items fetched so far.", file=sys.stderr)
                return all_items
            return None

        apps = data["data"]["apps"]
        items = apps.get("items") or []
        page_info = apps.get("pageInfo") or {}
        all_items.extend(items)

        total = page_info.get("totalCount", "?")
        print(f"  got {len(items)} items (total so far: {len(all_items)} / {total})", file=sys.stderr)

        if not page_info.get("hasNextPage"):
            break
        cursor = page_info.get("endCursor", "")

    return all_items


# ---------------------------------------------------------------------------
# Static JSON fallback
# ---------------------------------------------------------------------------

def fetch_static_json(session: requests.Session) -> list[dict]:
    """Fetch the static JSON game list from nvidiagrid.net."""
    print(f"\nFalling back to static JSON: {STATIC_JSON_URL}", file=sys.stderr)
    resp = session.get(STATIC_JSON_URL, timeout=30)
    resp.raise_for_status()
    games = resp.json()
    print(f"  got {len(games)} items from static JSON.", file=sys.stderr)
    return games


# ---------------------------------------------------------------------------
# Conversion to Google Play Game Actions DataFeed (schema.org JSON-LD)
# ---------------------------------------------------------------------------

def pick_image(item: dict) -> Optional[str]:
    """Pick the best available image URL from the item."""
    # GraphQL response: images is a dict of type -> url
    images = item.get("images")
    if isinstance(images, dict):
        for key in ("GAME_BOX_ART", "KEY_ART", "KEY_IMAGE", "HERO_IMAGE",
                    "FEATURE_IMAGE", "MARQUEE_HERO_IMAGE", "TV_BANNER"):
            val = images.get(key)
            if val:
                return val
    # Static JSON: may have an 'image' or 'imageUrl' field
    for key in ("image", "imageUrl", "boxArt"):
        val = item.get(key)
        if val:
            return val
    return None


def format_content_rating(item: dict) -> Optional[str]:
    """Extract a content-rating string from the item."""
    # GraphQL: contentRatings list of {type, categoryKey}
    content_ratings = item.get("contentRatings")
    if isinstance(content_ratings, list) and content_ratings:
        esrb = pegi = first = None
        for cr in content_ratings:
            rtype = (cr.get("type") or "").upper()
            cat = cr.get("categoryKey") or ""
            label = f"{rtype} {cat}".strip()
            if not first:
                first = label
            if rtype == "ESRB":
                esrb = label
            elif rtype == "PEGI":
                pegi = label
        return esrb or pegi or first
    return None


def build_play_url(item: dict) -> str:
    """Build a GeForce NOW deep-link URL."""
    game_id = item.get("cmsId") or item.get("id") or ""
    return f"https://play.geforcenow.com/mall/#/deeplink?game-id={game_id}"


def item_to_videogame(item: dict) -> dict:
    """Convert a GFN item (GraphQL or static JSON) to a schema.org VideoGame."""
    play_url = build_play_url(item)
    image_url = pick_image(item)
    content_rating = format_content_rating(item)

    vg: dict[str, Any] = {
        "@context": "http://schema.org",
        "@type": "VideoGame",
        "@id": f"gfn-{item.get('id', '')}",
        "name": item.get("title", ""),
        "url": play_url,
        "applicationCategory": "Game",
    }

    # Description (GraphQL: longDescription)
    desc = item.get("longDescription") or item.get("description")
    if desc:
        vg["description"] = desc

    genres = item.get("genres")
    if genres:
        vg["genre"] = genres if isinstance(genres, list) else [genres]

    if content_rating:
        vg["contentRating"] = content_rating

    if image_url:
        vg["image"] = image_url

    publisher = item.get("publisherName") or item.get("publisher")
    if publisher:
        vg["publisher"] = {"@type": "Organization", "name": publisher}

    developer = item.get("developerName") or item.get("developer")
    if developer:
        vg["contributor"] = {"@type": "Organization", "name": developer, "roleName": "developer"}

    keywords = item.get("keywords")
    if keywords:
        vg["keywords"] = keywords if isinstance(keywords, list) else [keywords]

    os_type = item.get("osType")
    if os_type:
        vg["gamePlatform"] = [os_type]

    for player_key in ("maxOnlinePlayers", "maxLocalPlayers"):
        val = item.get(player_key)
        if val:
            vg["numberOfPlayers"] = val

    play_modes = item.get("supportedGamePlayModes")
    if play_modes:
        vg["playMode"] = play_modes

    # Store cross-references
    same_as: list[str] = []
    for sid in (item.get("storeIds") or []):
        store = (sid.get("store") or "").upper()
        sid_val = sid.get("id") or ""
        if store == "STEAM" and sid_val:
            same_as.append(f"https://store.steampowered.com/app/{sid_val}")
        elif store == "EPIC" and sid_val:
            same_as.append(f"https://store.epicgames.com/p/{sid_val}")
    # Static JSON fallback: steamUrl field
    steam_url = item.get("steamUrl")
    if steam_url and steam_url not in same_as:
        same_as.append(steam_url)
    if same_as:
        vg["sameAs"] = same_as

    # Release date
    computed = item.get("computedValues") or {}
    release = computed.get("earliestReleaseDate") or computed.get("earliestStreetDate")
    if release:
        vg["datePublished"] = release

    # --- VideoGame (Edition) with PlayGameAction ---
    edition: dict[str, Any] = {
        "@context": "http://schema.org",
        "@type": "VideoGame",
        "name": f"{item.get('title', '')} (GeForce NOW)",
        "gamePlatform": ["PC"],
        "potentialAction": {
            "@type": "PlayGameAction",
            "target": {
                "@type": "EntryPoint",
                "urlTemplate": play_url,
                "actionPlatform": ["http://schema.org/DesktopWebPlatform"],
            },
        },
    }

    editions = [edition]
    for variant in (item.get("variants") or []):
        gfn_info = variant.get("gfn") or {}
        status = gfn_info.get("status")
        if status and status not in ("AVAILABLE", "MAINTENANCE"):
            continue
        v_store = variant.get("appStore") or ""
        v_title = variant.get("title") or item.get("title", "")
        var_ed: dict[str, Any] = {
            "@context": "http://schema.org",
            "@type": "VideoGame",
            "name": f"{v_title} ({v_store})" if v_store else v_title,
            "gamePlatform": ["PC"],
        }
        if v_store:
            var_ed["applicationCategory"] = v_store
        editions.append(var_ed)

    vg["exampleOfWork"] = editions
    return vg


def build_datafeed(games: list[dict]) -> dict:
    """Wrap VideoGame entities into a DataFeed envelope."""
    return {
        "@context": "http://schema.org",
        "@type": "DataFeed",
        "dateModified": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "dataFeedElement": games,
    }


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> None:
    parser = argparse.ArgumentParser(
        description="Scrape NVIDIA GeForce NOW games → Google Play Game Actions DataFeed JSON."
    )
    parser.add_argument("-o", "--output", default=None, help="Output file (default: stdout)")
    parser.add_argument("--raw", action="store_true", help="Also save raw API response to <output>.raw.json")
    parser.add_argument("--indent", type=int, default=2, help="JSON indent (default: 2)")
    parser.add_argument("--static-only", action="store_true", help="Skip GraphQL, use static JSON only")
    args = parser.parse_args()

    session = requests.Session()
    session.headers.update({
        "Content-Type": "application/json",
        "Accept": "application/json",
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
    })

    raw_games = None

    if not args.static_only:
        print("=== Trying GraphQL API ===", file=sys.stderr)
        raw_games = fetch_all_graphql(session)

    if raw_games is None:
        print("\n=== Using static JSON fallback ===", file=sys.stderr)
        raw_games = fetch_static_json(session)

    print(f"\nTotal items: {len(raw_games)}", file=sys.stderr)

    # Save raw data
    if args.raw and args.output:
        raw_path = args.output + ".raw.json"
        with open(raw_path, "w", encoding="utf-8") as f:
            json.dump(raw_games, f, indent=args.indent, ensure_ascii=False)
        print(f"Raw data → {raw_path}", file=sys.stderr)

    # Convert
    feed = build_datafeed([item_to_videogame(g) for g in raw_games])
    output_json = json.dumps(feed, indent=args.indent, ensure_ascii=False)

    if args.output:
        with open(args.output, "w", encoding="utf-8") as f:
            f.write(output_json + "\n")
        print(f"DataFeed → {args.output}", file=sys.stderr)
    else:
        print(output_json)

    print("Done.", file=sys.stderr)


if __name__ == "__main__":
    main()
