import requests
import json
import time
import math
import csv
import datetime
import signal
import sys
from concurrent.futures import ThreadPoolExecutor, as_completed
from urllib.parse import urlencode

# === CONFIGURATION ===
BASE_URL = "https://bina.az/graphql"


PROXY_URL = "http://spi8e1zees:HX83+0fdkrhNouutR4@az.decodo.com:30001" # <--- EDIT THIS LINE

PROXIES = { "http": PROXY_URL, "https": PROXY_URL } if "YOUR_FULL_PASSWORD" not in PROXY_URL else {}

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

# === MODIFIED FUNCTION TO USE PROXY ===
def graphql_request(operation_name: str, variables: dict, sha256: str) -> dict:
    HEADERS["X-APOLLO-OPERATION-NAME"] = operation_name
    extensions = {"persistedQuery": {"version": 1, "sha256Hash": sha256}}
    
    # The only change is adding the 'proxies' parameter and increasing the timeout
    resp = SESSION.get(
        BASE_URL,
        headers=HEADERS,
        params={
            "operationName": operation_name,
            "variables": json.dumps(variables),
            "extensions": json.dumps(extensions)
        },
        proxies=PROXIES, # <--- THIS LINE ACTIVATES THE PROXY
        timeout=45      # <--- Increased timeout is safer for proxies
    )
    resp.raise_for_status()
    return resp.json()

# Fetch functions (No changes needed, they use the modified graphql_request)

def get_total_count(filter_params: dict) -> int:
    data = graphql_request("SearchTotalCount", {"filter": filter_params}, HASHES["count"])
    return data.get("data", {}).get("itemsConnection", {}).get("totalCount", 0)


def fetch_batch(offset: int, limit: int = 24) -> list:
    data = graphql_request("FeaturedItemsRow", {"limit": limit, "offset": offset}, HASHES["list"])
    return data.get("data", {}).get("items", [])


def fetch_detail(item_id: str) -> dict:
    data = graphql_request("CurrentItem", {"id": item_id}, HASHES["detail"])
    return data.get("data", {}).get("item", {})

# Parsers (No changes needed)

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
    phones = item.get("phones") or []
    phone_list = [p.get("value") for p in phones if p.get("value")]
    category = item.get("category") or {}
    floor = item.get("floor")
    floors = item.get("floors")
    has_deed = item.get("hasBillOfSale", False)
    has_repair = item.get("hasRepair", False)

    return {
        "description":  item.get("description"),
        "address":      item.get("address"),
        "latitude":     item.get("latitude"),
        "longitude":    item.get("longitude"),
        "contact_name": item.get("contactName"),
        "phones":       ", ".join(phone_list),
        "category":     category.get("name"),
        "Çıxarış":      "Yes" if has_deed else "No",
        "Təmir":        "Yes" if has_repair else "No",
        "Mərtəbə":      f"{floor}/{floors}" if floor is not None and floors is not None else None,
    }

# Combined fetch+parse detail (No changes needed)

def fetch_and_parse_detail(item_meta: dict) -> dict:
    detail = fetch_detail(item_meta["id"])
    detail_parsed = parse_detail_fields(detail)
    item_meta.update(detail_parsed)
    return item_meta

# Global variable to store scraped data
scraped_data = []

# Save and Signal Handling (No changes needed)

def save_data(data, reason="completed"):
    if not data:
        print(f"No data to save ({reason})")
        return
    
    keys = data[0].keys()
    date_str = datetime.datetime.now().strftime("%Y%m%d")
    filename = f"bina_listings_{date_str}.csv"
    
    with open(filename, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=keys)
        writer.writeheader()
        writer.writerows(data)
    
    print(f"Data saved to {filename} ({len(data)} items, {reason})")

def signal_handler(sig, frame):
    print('\nKeyboard interrupt received. Saving scraped data...')
    save_data(scraped_data, "interrupted")
    sys.exit(0)

signal.signal(signal.SIGINT, signal_handler)

# Main scraper (No changes needed, but will now use the proxy via graphql_request)
def main():
    global scraped_data
    
    if not PROXIES:
        print("!!! WARNING: Proxy is not configured. Running on your own IP. !!!")
        print("!!! Edit the PROXY_URL variable to use a proxy. !!!")
        
    filter_params = {}
    try:
        total = get_total_count(filter_params)
    except Exception as e:
        print(f"Failed to get total count. Your IP may be blocked or the proxy is not working.")
        print(f"Error: {e}")
        sys.exit(1)
        
    limit = 24
    pages = math.ceil(total / limit)
    print(f"Total listings: {total}, pages: {pages}")

    scraped_data = []
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        for i in range(pages):
            offset = i * limit
            print(f"Batch {i+1}/{pages} (offset={offset})")
            
            # This loop will stop if the page limit is hit
            if i >= 47:
                print("Stopping due to 47-page limit of this API endpoint.")
                break
                
            try:
                batch = fetch_batch(offset, limit)
            except Exception as e:
                print(f"Error fetching batch {i+1}: {e}. Skipping.")
                continue

            if not batch:
                print("Received empty batch. Ending scrape.")
                break

            metas = [parse_listing(item) for item in batch]
            futures = [executor.submit(fetch_and_parse_detail, meta) for meta in metas]
            for future in as_completed(futures):
                try:
                    scraped_data.append(future.result())
                except Exception as e:
                    print(f"Detail fetch error: {e}")

            time.sleep(BATCH_DELAY)

    save_data(scraped_data, "completed")

if __name__ == "__main__":
    main()