import requests
import json
import time
import math
import csv
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
    # Optional: include cookies if needed
    # "Cookie": "_binaaz_session=...; other=...",
}

HASHES = {
    "list":   "f34b27afebc725b2bb62b62f9757e1740beaf2dc162f4194e29ba5a608b3cb41",
    "count":  "9869b12c312f3c3ca3f7de0ced1f6fcb355781db43f49b4d8b3e278c13490ae6",
    "detail": "0b96ba66315ed1a9e29f46018744ff8311996007dd6397a073cf59c755596f84",
}

# === GRAPHQL REQUEST FUNCTION ===
def graphql_request(operation_name: str, variables: dict, sha256: str) -> dict:
    HEADERS["X-APOLLO-OPERATION-NAME"] = operation_name
    payload_extensions = {"persistedQuery": {"version": 1, "sha256Hash": sha256}}

    resp = requests.get(
        BASE_URL,
        headers=HEADERS,
        params={
            "operationName": operation_name,
            "variables": json.dumps(variables),
            "extensions": json.dumps(payload_extensions)
        },
        timeout=30
    )
    resp.raise_for_status()
    return resp.json()

# === FETCH FUNCTIONS ===
def get_total_count(filter_params: dict) -> int:
    data = graphql_request("SearchTotalCount", {"filter": filter_params}, HASHES["count"])
    return data.get("data", {}).get("itemsConnection", {}).get("totalCount", 0)


def fetch_batch(offset: int, limit: int = 24) -> list:
    data = graphql_request("FeaturedItemsRow", {"limit": limit, "offset": offset}, HASHES["list"])
    return data.get("data", {}).get("items", [])


def fetch_detail(item_id: str) -> dict:
    data = graphql_request("CurrentItem", {"id": item_id}, HASHES["detail"])
    return data.get("data", {}).get("item", {})

# === PARSERS ===
def parse_listing(item: dict) -> dict:
    loc = item.get("location") or {}
    city = item.get("city") or {}
    price = item.get("price") or {}
    area = item.get("area") or {}

    return {
        "id":           item.get("id"),
        "url":          f"https://bina.az{item.get('path', '')}",
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


def parse_detail(item: dict) -> dict:
    phones = item.get("phones") or []
    phone_list = [p.get("value") for p in phones if p.get("value")]

    return {
        "description":  item.get("description"),
        "address":      item.get("address"),
        "latitude":     item.get("latitude"),
        "longitude":    item.get("longitude"),
        "contact_name": item.get("contactName"),
        "phones":       ", ".join(phone_list),
    }

# === MAIN SCRAPER ===
def main():
    # Define filters (e.g., city=1, category=1)
    filter_params = {"cityId": "1", "categoryId": "1", "leased": False}

    total = get_total_count(filter_params)
    limit = 24
    pages = math.ceil(total / limit)
    print(f"Total listings: {total}, pages: {pages}")

    out_data = []

    for i in range(pages):
        offset = i * limit
        print(f"Batch {i+1}/{pages} (offset={offset})")

        batch = fetch_batch(offset, limit)
        if not batch:
            break

        for item in batch:
            meta = parse_listing(item)
            detail = fetch_detail(item.get("id"))
            meta.update(parse_detail(detail))
            out_data.append(meta)

        time.sleep(0.3)

    # Write to CSV
    if out_data:
        keys = out_data[0].keys()
        with open("bina_listings.csv", "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=keys)
            writer.writeheader()
            writer.writerows(out_data)

    print("Done. Data saved to bina_listings.csv")

if __name__ == "__main__":
    main()
