import pandas as pd
import time
import random
import tempfile
import shutil
import os
from selenium import webdriver
from selenium.webdriver.chrome.service import Service as ChromeService
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, WebDriverException
from bs4 import BeautifulSoup

# --- Configuration ---
BASE_URL = "https://tap.az"
MAIN_CATEGORY_URL = "/elanlar/neqliyyat/tikinti-texnikasi"
USER_AGENT = 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/138.0.0.0 Safari/537.36 Edg/138.0.0.0'
OUTPUT_FILENAME = "tap_az_tikinti_texnikasi_final.xlsx"
BROWSER_RESTART_BATCH_SIZE = 50 


def setup_driver():
    """Initializes and returns a Selenium WebDriver instance with a unique user data directory."""
    print("Initializing a fresh Selenium WebDriver instance...")
    options = Options()
    user_data_dir = tempfile.mkdtemp()
    options.add_argument(f"--user-data-dir={user_data_dir}")
    options.add_argument("--headless=new")
    options.add_argument("--disable-gpu")
    options.add_argument("--window-size=1920,1200")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument(f"user-agent={USER_AGENT}")
    options.add_experimental_option('excludeSwitches', ['enable-logging'])
    
    service = ChromeService(executable_path="chromedriver.exe")
    driver = webdriver.Chrome(service=service, options=options)
    driver.user_data_dir = user_data_dir
    return driver

def cleanup_driver(driver):
    """Quits the driver and cleans up its temporary user data directory."""
    if driver:
        user_data_dir = getattr(driver, 'user_data_dir', None)
        try:
            driver.quit()
        except Exception:
            pass 
        if user_data_dir:
            shutil.rmtree(user_data_dir, ignore_errors=True)

def get_subcategory_urls(driver, main_category_url):
    print(f"Fetching subcategories from: {BASE_URL + main_category_url}")
    try:
        driver.get(BASE_URL + main_category_url)
        WebDriverWait(driver, 10).until(EC.presence_of_element_located((By.CSS_SELECTOR, ".subcategories-inner")))
        soup = BeautifulSoup(driver.page_source, 'html.parser')
        sub_category_urls = [{'name': link.text.strip(), 'url': link.get('href')} for link in soup.select('.subcategories-inner a.cat-name') if link.get('href')]
        print(f"Found {len(sub_category_urls)} subcategories.")
        return sub_category_urls
    except TimeoutException:
        print("Error: Could not find subcategories on the main page.")
        return []

def get_product_urls_from_subcategory(driver, subcategory):
    full_subcategory_url = BASE_URL + subcategory['url']
    driver.get(full_subcategory_url)
    js_click_script = "let btn = document.querySelector('.pagination .next a'); if (btn) { btn.scrollIntoView({block: 'center'}); btn.click(); return true; } return false;"
    
    page_count = 1
    while True:
        try:
            clicked = driver.execute_script(js_click_script)
            if not clicked:
                print("  - 'Show More' button not found by JS. Assuming all products are loaded.")
                break
            page_count += 1
            print(f"  - Clicking 'Show More'... (Page {page_count})")
            WebDriverWait(driver, 5).until(EC.visibility_of_element_located((By.CLASS_NAME, "pagination_loading")))
            WebDriverWait(driver, 15).until(EC.invisibility_of_element_located((By.CLASS_NAME, "pagination_loading")))
        except TimeoutException:
            print("  - Loading process finished for this subcategory.")
            break
            
    soup = BeautifulSoup(driver.page_source, 'html.parser')
    return list(set(BASE_URL + link.get('href') for link in soup.select('div.products-i a.products-link') if link.get('href') and '/elanlar/' in link.get('href')))

def scrape_product_details(driver, url):
    try:
        driver.get(url)
        WebDriverWait(driver, 10).until(EC.visibility_of_element_located((By.CSS_SELECTOR, "h1.product-title")))
        soup = BeautifulSoup(driver.page_source, 'html.parser')

        def get_text(element): return element.text.strip() if element else None
        def get_property_value(label_text):
            for prop in soup.select('.product-properties__i'):
                label = prop.select_one('.product-properties__i-name')
                if label and label_text in label.text:
                    value_el = prop.select_one('.product-properties__i-value')
                    return get_text(value_el.find('a')) if value_el and value_el.find('a') else get_text(value_el)
            return None

        price_val = get_text(soup.select_one('.price-val'))
        description_el = soup.select_one('.product-description__content')
        return {'Type of Product': "Tikinti Texnikası", 'elan_id': url.split('/')[-1].split('?')[0], 'title': get_text(soup.select_one('h1.product-title')), 'price': int(''.join(filter(str.isdigit, price_val))) if price_val else None, 'city': get_property_value('Şəhər'), 'category': get_property_value('Malın növü'), 'Year': get_property_value('Buraxılış ili'), 'New?': get_property_value('Yeni?'), 'Yurusu_km': get_property_value('Yürüşü, km'), 'Description': ' '.join(description_el.stripped_strings) if description_el else None, 'URL': url}
    except WebDriverException as e: 
        print(f"    - CRITICAL BROWSER ERROR on {url}: {e.args[0].splitlines()[0]}")
        raise 
    except Exception as e:
        print(f"    - Error parsing detail page {url}: {e}")
        return None

if __name__ == "__main__":
    if os.path.exists(OUTPUT_FILENAME):
        os.remove(OUTPUT_FILENAME)
        print(f"Removed old output file: {OUTPUT_FILENAME}")
    
    # Get subcategories just once at the start
    temp_driver = setup_driver()
    try:
        subcategories = get_subcategory_urls(temp_driver, MAIN_CATEGORY_URL)
    finally:
        cleanup_driver(temp_driver)
        print("Temporary driver for subcategory listing closed.")

    if not subcategories:
        print("Could not get subcategories. Exiting.")
    else:
        for subcat in subcategories:
            print(f"\n{'='*20} Processing subcategory: '{subcat['name']}' {'='*20}")
            driver = setup_driver()
            try:
                product_links = get_product_urls_from_subcategory(driver, subcat)
                print(f"Found {len(product_links)} products in '{subcat['name']}'. Now scraping their details...")
                
                subcategory_data = []
                products_scraped_since_restart = 0
                
                for i, link in enumerate(product_links, 1):
                    try:
                        print(f"  - Scraping product {i}/{len(product_links)}: {link}")
                        details = scrape_product_details(driver, link)
                        if details:
                            subcategory_data.append(details)
                        products_scraped_since_restart += 1
                        
                        # Proactively restart the browser after a set number of scrapes
                        if products_scraped_since_restart >= BROWSER_RESTART_BATCH_SIZE:
                            print(f"\n--- Reached batch size of {BROWSER_RESTART_BATCH_SIZE}. Restarting browser to maintain stability... ---\n")
                            cleanup_driver(driver)
                            driver = setup_driver()
                            products_scraped_since_restart = 0

                        time.sleep(random.uniform(0.5, 1.5))
                    
                    except WebDriverException:
                        print(f"\n--- Browser crashed. Restarting driver and continuing... ---\n")
                        cleanup_driver(driver)
                        driver = setup_driver()
                        products_scraped_since_restart = 0
                        # Optional: Retry the failed link one more time
                        print(f"  - Retrying last failed product: {link}")
                        details = scrape_product_details(driver, link)
                        if details:
                            subcategory_data.append(details)


                if subcategory_data:
                    df_new = pd.DataFrame(subcategory_data)
                    if not os.path.exists(OUTPUT_FILENAME):
                        df_new.to_excel(OUTPUT_FILENAME, index=False)
                        print(f"Created '{OUTPUT_FILENAME}' and saved {len(subcategory_data)} records.")
                    else:
                        with pd.ExcelWriter(OUTPUT_FILENAME, mode='a', engine='openpyxl', if_sheet_exists='overlay') as writer:
                            df_new.to_excel(writer, header=False, index=False, startrow=writer.sheets['Sheet1'].max_row)
                        print(f"Appended {len(subcategory_data)} new records to '{OUTPUT_FILENAME}'.")
                
            except Exception as e:
                print(f"A critical error occurred while processing '{subcat['name']}'. Moving on. Error: {e}")
            finally:
                cleanup_driver(driver)
                print(f"Driver for '{subcat['name']}' closed.")

    print("\nScript finished.")