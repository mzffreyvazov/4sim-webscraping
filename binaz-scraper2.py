import requests
import json
import time
import math
import csv
import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
from urllib.parse import urlencode

# === CONFIGURATION ===
BASE_URL = "https://bina.az/graphql"
HEADERS = {
    "User-Agent": "Mozilla/5.0 (compatible; BinaScraper/1.0)",
    "Accept": "*/*",
    "Content-Type": "application/json",
    "Referer": "https://bina.az/",
    "X-Requested-With": "XMLHttpRequest",
    "X-APOLLO-OPERATION-NAME": "",
}
# Number of threads for parallel detail fetching
MAX_WORKERS = 10
# Delay between batch requests (seconds)
BATCH_DELAY = 0.2

HASHES = {
    "list":   "f34b27afebc725b2bb62b62f9757e1740beaf2dc162f4194e29ba5a608b3cb41",
    "count":  "9869b12c312f3c3ca3f7de0ced1f6fcb355781db43f49b4d8b3e278c13490ae6",
    "detail": "0b96ba66315ed1a9e29f46018744ff8311996007dd6397a073cf59c755596f84",
}

# Use session for connection pooling
SESSION = requests.Session()

def graphql_request(operation_name: str, variables: dict, sha256: str) -> dict:
    HEADERS["X-APOLLO-OPERATION-NAME"] = operation_name
    extensions = {"persistedQuery": {"version": 1, "sha256Hash": sha256}}
    resp = SESSION.get(
        BASE_URL,
        headers=HEADERS,
        params={
            "operationName": operation_name,
            "variables": json.dumps(variables),
            "extensions": json.dumps(extensions)
        },
        timeout=30
    )
    resp.raise_for_status()
    return resp.json()

# Fetch functions

def get_total_count(filter_params: dict) -> int:
    data = graphql_request("SearchTotalCount", {"filter": filter_params}, HASHES["count"])
    return data.get("data", {}).get("itemsConnection", {}).get("totalCount", 0)


def fetch_batch(offset: int, limit: int = 24) -> list:
    data = graphql_request("FeaturedItemsRow", {"limit": limit, "offset": offset}, HASHES["list"])
    return data.get("data", {}).get("items", [])


def fetch_detail(item_id: str) -> dict:
    data = graphql_request("CurrentItem", {"id": item_id}, HASHES["detail"])
    return data.get("data", {}).get("item", {})

# Parsers

def parse_listing(item: dict) -> dict:
    loc = item.get("location") or {}
    city = item.get("city") or {}
    price = item.get("price") or {}
    area = item.get("area") or {}
    return {
        "id":           item.get("id"),
        "url":          f"https://bina.az{item.get('path','')}",
        "price":        price.get("value"),
        "currency":     price.get("currency"),
        "rooms":        item.get("rooms"),
        "area":         area.get("value"),
        "area_units":   area.get("units"),
        "location":     loc.get("fullName"),
        "city":         city.get("name"),
        "updated_at":   item.get("updatedAt"),
        "photos_count": item.get("photosCount"),
    }


def parse_detail_fields(item: dict) -> dict:
    # Basic details
    phones = item.get("phones") or []
    phone_list = [p.get("value") for p in phones if p.get("value")]
    # New requested fields
    category = item.get("category") or {}
    floor = item.get("floor")
    floors = item.get("floors")
    has_deed = item.get("hasBillOfSale", False)
    has_repair = item.get("hasRepair", False)

    return {
        # Existing fields
        "description":  item.get("description"),
        "address":      item.get("address"),
        "latitude":     item.get("latitude"),
        "longitude":    item.get("longitude"),
        "contact_name": item.get("contactName"),
        "phones":       ", ".join(phone_list),
        # Category name (Yeni tikili, Köhnə tikili, etc.)
        "category":     category.get("name"),
        # Deed available?  → Çıxarış
        "Çıxarış":      "Yes" if has_deed else "No",
        # Renovation? → Təmir
        "Təmir":        "Yes" if has_repair else "No",
        # Floor info → Mərtəbə
        "Mərtəbə":      f"{floor}/{floors}" if floor is not None and floors is not None else None,
    }

# Combined fetch+parse detail

def fetch_and_parse_detail(item_meta: dict) -> dict:
    detail = fetch_detail(item_meta["id"])
    detail_parsed = parse_detail_fields(detail)
    item_meta.update(detail_parsed)
    return item_meta

# Main scraper
def main():
    filter_params = {}
    total = get_total_count(filter_params)
    limit = 24
    pages = math.ceil(total / limit)
    print(f"Total listings: {total}, pages: {pages}")

    out_data = []
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        for i in range(pages):
            offset = i * limit
            print(f"Batch {i+1}/{pages} (offset={offset})")
            batch = fetch_batch(offset, limit)
            if not batch:
                break

            # Parse metadata
            metas = [parse_listing(item) for item in batch]
            # Parallel detail fetch + parse
            futures = [executor.submit(fetch_and_parse_detail, meta) for meta in metas]
            for future in as_completed(futures):
                try:
                    out_data.append(future.result())
                except Exception as e:
                    print(f"Detail fetch error: {e}")

            time.sleep(BATCH_DELAY)

    # Write CSV
    if out_data:
        keys = out_data[0].keys()
        date_str = datetime.datetime.now().strftime("%Y%m%d")
        filename = f"bina_listings_{date_str}.csv"
        with open(filename, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=keys)
            writer.writeheader()
            writer.writerows(out_data)

    print(f"Done. Data saved to {filename}")

if __name__ == "__main__":
    main()
