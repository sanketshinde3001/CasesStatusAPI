import os
import time
import datetime
import io
import re # For Gemini solution extraction
import logging # For logging
import calendar # For getting last day of month
from PIL import Image
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service as ChromeService
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException 
from webdriver_manager.chrome import ChromeDriverManager
from bs4 import BeautifulSoup
import pymongo
import google.generativeai as genai

# --- Logging Setup ---
log_formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(module)s:%(lineno)d - %(message)s')
log_file = 'sci_scraper_monthly.log' # New log file name

file_handler = logging.FileHandler(log_file, mode='a')
file_handler.setFormatter(log_formatter)
file_handler.setLevel(logging.INFO)

console_handler = logging.StreamHandler()
console_handler.setFormatter(log_formatter)
console_handler.setLevel(logging.INFO)

logger = logging.getLogger()
logger.setLevel(logging.INFO)
if not logger.handlers:
    logger.addHandler(file_handler)
    logger.addHandler(console_handler)

# --- Configuration ---
WEBSITE_URL = "https://www.sci.gov.in/judgements-judgement-date/"
GEMINI_API_KEYS = [
    "AIzaSyD4jbBDqK0c43zdfcTWGvZE3zgToK9u0Lo",
    "AIzaSyA30yQWEVP-2BRruxTplHrM_jqnPN9l91M",
    "AIzaSyAwQa8N_vI0Dc8sdtsuFrYc9STbHwpsRF4",
    "AIzaSyCfgTZ9nKabXZKXcctn2SiRftsA7tVXOEo",
    "AIzaSyDJtRHN2IPw34SWYGccykHPgQPsLIs-fWw",
    "AIzaSyCMvwHi_wq_e6CH3M2Ueya_QwYzQUQxw6A"
]
MONGODB_CONNECTION_STRING = "mongodb+srv://sanketshinde3123:n2rXcex2EDQPk96G@scrapper.fqsbnrc.mongodb.net/?retryWrites=true&w=majority&appName=scrapper"
MONGODB_DATABASE_NAME = "sci_judgements"
MONGODB_COLLECTION_NAME = "judgements_monthly" # Potentially new collection for monthly scrapes
PROCESSED_MONTHS_COLLECTION_NAME = "processed_months" # Changed from dates to months

current_gemini_api_key_index = 0

def get_last_day_of_month(year, month):
    return calendar.monthrange(year, month)[1]

def get_gemini_api_key():
    # ... (same as before)
    global current_gemini_api_key_index
    if not GEMINI_API_KEYS or GEMINI_API_KEYS[0].startswith("YOUR_GEMINI_API_KEY"): # Check if placeholder still there
        logger.critical("Gemini API keys are not configured properly. Default placeholder detected.")
        # You might want to raise an error or exit if API keys are mandatory and not set
        # For now, let's assume they are correctly replaced by the user in their actual script
        # raise ValueError("Gemini API keys are not configured properly.") 
    key = GEMINI_API_KEYS[current_gemini_api_key_index]
    current_gemini_api_key_index = (current_gemini_api_key_index + 1) % len(GEMINI_API_KEYS)
    return key


def configure_gemini():
    # ... (same as before)
    api_key = get_gemini_api_key()
    genai.configure(api_key=api_key)
    logger.info(f"Configured Gemini with API key ending: ...{api_key[-4:]}")

def solve_captcha_with_gemini(image_bytes):
    # ... (same as before)
    if not image_bytes:
        logger.warning("No image bytes provided to Gemini solver.")
        return None
    try:
        configure_gemini()
        model = genai.GenerativeModel('gemini-2.0-flash')
        img_part = {"mime_type": "image/png", "data": image_bytes}
        prompt = "Solve the math problem in this image. Provide only the numerical answer. For example, if the image shows '5 + 3', respond with '8'."
        logger.info("Sending CAPTCHA to Gemini...")
        response = model.generate_content([prompt, img_part])
        if response.parts:
            solution = response.text.strip()
            logger.info(f"Gemini raw solution: '{solution}'")
            match = re.search(r'-?\d+', solution)
            if match:
                numeric_solution = match.group(0)
                logger.info(f"Gemini extracted numeric solution: {numeric_solution}")
                return numeric_solution
            else:
                logger.warning(f"Gemini returned non-numeric solution: {solution}")
                return None
        else:
            logger.warning(f"Gemini returned no parts in response. Full Response: {response}")
            return None
    except Exception as e:
        logger.error(f"Error solving CAPTCHA with Gemini: {e}", exc_info=True)
        return None

def setup_driver():
    # ... (same as before)
    options = webdriver.ChromeOptions()
    options.add_argument("--start-maximized")
    # options.add_argument("--headless")
    options.add_argument("--disable-gpu")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_experimental_option('excludeSwitches', ['enable-logging'])
    service = ChromeService(ChromeDriverManager().install())
    driver = webdriver.Chrome(service=service, options=options)
    logger.info("WebDriver setup complete.")
    return driver


def connect_to_mongodb():
    client = pymongo.MongoClient(MONGODB_CONNECTION_STRING)
    db = client[MONGODB_DATABASE_NAME]
    judgements_collection = db[MONGODB_COLLECTION_NAME]
    processed_months_coll = db[PROCESSED_MONTHS_COLLECTION_NAME] # Changed
    
    # Index for unique judgements based on diary_number and actual_judgement_date
    judgements_collection.create_index(
        [("diary_number", pymongo.ASCENDING), ("actual_judgement_date", pymongo.ASCENDING)], 
        unique=True, sparse=True
    )
    processed_months_coll.create_index("month_year_str", unique=True) # For YYYY-MM
    logger.info(f"Connected to MongoDB. DB: {MONGODB_DATABASE_NAME}, Collections: {MONGODB_COLLECTION_NAME}, {PROCESSED_MONTHS_COLLECTION_NAME}")
    return judgements_collection, processed_months_coll

def is_month_processed(month_year_str, processed_months_coll): # month_year_str e.g., "2025-05"
    return processed_months_coll.count_documents({"month_year_str": month_year_str}) > 0

def mark_month_as_processed(month_year_str, processed_months_coll): # month_year_str e.g., "2025-05"
    try:
        processed_months_coll.insert_one({"month_year_str": month_year_str, "processed_at": datetime.datetime.utcnow()})
        logger.info(f"Marked month {month_year_str} as processed.")
    except pymongo.errors.DuplicateKeyError:
        logger.info(f"Month {month_year_str} was already marked as processed.")

def parse_judgement_data(html_content, search_month_year_str): # search_month_year_str e.g., "05-2025"
    logger.info(f"Starting to parse judgement data for search month: {search_month_year_str}")
    soup = BeautifulSoup(html_content, 'html.parser')
    judgements = []
    results_table_container = soup.find('div', class_='distTableContent')
    if not results_table_container:
        logger.warning(f"Could not find 'div.distTableContent' for search month {search_month_year_str}.")
        debug_html_path = f"DEBUG_no_table_container_{search_month_year_str.replace('-', '_')}.html"
        with open(debug_html_path, "w", encoding="utf-8") as f_debug_html: f_debug_html.write(html_content)
        logger.info(f"Saved HTML for debugging missing table container: {debug_html_path}")
        return judgements

    table = results_table_container.find('table')
    if not table:
        logger.warning(f"No <table> found within 'div.distTableContent' for search month: {search_month_year_str}")
        return judgements
    
    tbody = table.find('tbody')
    if not tbody:
        logger.warning(f"No <tbody> found in results table for search month: {search_month_year_str}")
        return judgements

    rows = tbody.find_all('tr')
    logger.info(f"Found {len(rows)} rows in the table for search month: {search_month_year_str}")
    if not rows: return judgements

    parsed_judgements_count_for_month = 0
    for row_idx, row in enumerate(rows):
        cells = row.find_all('td')
        if len(cells) < 8: 
            logger.warning(f"Skipping row {row_idx+1} (search month: {search_month_year_str}): {len(cells)} cells (expected 8). Content: {row.get_text(separator='|', strip=True)}")
            continue
        try:
            def get_cell_content_text(cell):
                # ... (same robust get_cell_content_text as before)
                if not cell: return ""
                bt_content_span = cell.find('span', class_='bt-content')
                if bt_content_span:
                    if cell.get('class') and ('petitioners' in cell.get('class') or 'advocate' in cell.get('data-th', '').lower()):
                        divs = bt_content_span.find_all('div')
                        if divs: return ' '.join(div.text.strip() for div in divs if div.text.strip())
                    return ' '.join(bt_content_span.text.split())
                return ' '.join(cell.text.split())

            diary_no = row.get('data-diary-no', '').strip() or get_cell_content_text(cells[1])
            if not diary_no:
                logger.warning(f"Skipping row {row_idx+1} (search month: {search_month_year_str}) due to missing Diary Number.")
                continue

            # Extract actual judgement date from PDF link text (e.g., "09-05-2025(English)")
            actual_judgement_date_str = None
            judgment_links_cell_content = cells[7].find('span', class_='bt-content') or cells[7]
            first_link_tag = judgment_links_cell_content.find('a', href=True)
            if first_link_tag and first_link_tag.text:
                match_date = re.match(r'(\d{2}-\d{2}-\d{4})', first_link_tag.text.strip())
                if match_date:
                    actual_judgement_date_str = match_date.group(1)
            
            if not actual_judgement_date_str:
                 # Fallback: Try to parse from PDF link filename
                if first_link_tag and first_link_tag.get('href'):
                    href_match = re.search(r'_(\d{2}-[A-Za-z]{3}-\d{4})\.pdf$', first_link_tag.get('href'))
                    if href_match:
                        date_from_href = href_match.group(1) # e.g., 09-May-2025
                        try:
                            # Convert "09-May-2025" to "09-05-2025"
                            dt_obj = datetime.datetime.strptime(date_from_href, "%d-%b-%Y")
                            actual_judgement_date_str = dt_obj.strftime("%d-%m-%Y")
                        except ValueError:
                            logger.warning(f"Could not parse date '{date_from_href}' from href for Diary No: {diary_no}")
                
                if not actual_judgement_date_str: # If still not found, this row might be problematic for unique ID
                    logger.warning(f"Could not determine actual_judgement_date for row {row_idx+1} (Diary No: {diary_no}), search month {search_month_year_str}. Skipping row for data integrity with _id.")
                    continue 

            bench_text = get_cell_content_text(cells[5]).replace('<br>', '; ').replace('\n', '; ')
            bench_text = ' '.join(bench_text.split())

            judgement_detail = {
                "serial_number": get_cell_content_text(cells[0]),
                "diary_number": diary_no,
                "case_number": get_cell_content_text(cells[2]),
                "petitioner_respondent": get_cell_content_text(cells[3]),
                "petitioner_respondent_advocate": get_cell_content_text(cells[4]),
                "bench": bench_text,
                "judgment_by": get_cell_content_text(cells[6]),
                "actual_judgement_date": actual_judgement_date_str, # Store the extracted actual date
                "search_query_month_year": search_month_year_str, # e.g., "05-2025"
                "scraped_at_utc": datetime.datetime.utcnow()
            }
            
            judgment_links_html = judgment_links_cell_content.find_all('a', href=True)
            judgment_links = []
            for link_tag in judgment_links_html:
                href = link_tag['href']
                text = ' '.join(link_tag.text.strip().replace('\n', ' ').replace('<br>', ' ').split())
                if href and "api.sci.gov.in" in href and text: 
                    judgment_links.append({"text": text, "url": href})
            judgement_detail["judgment_links"] = judgment_links
            
            judgement_detail["_id"] = f"{diary_no}_{actual_judgement_date_str}" # More robust _id
            judgements.append(judgement_detail)
            parsed_judgements_count_for_month += 1
            logger.debug(f"Parsed row {row_idx+1} (Diary No: {diary_no}, Actual Date: {actual_judgement_date_str}) for search month {search_month_year_str}.")
        except Exception as e:
            logger.error(f"Error parsing row {row_idx+1} (search month {search_month_year_str}). Error: {e}", exc_info=True)
            continue
            
    logger.info(f"Finished parsing. Extracted {parsed_judgements_count_for_month} judgements for search month: {search_month_year_str}")
    return judgements

def save_judgements_to_mongodb(judgements, collection):
    # ... (same as before, should handle upserts correctly with the new _id)
    if not judgements:
        return 0
    saved_count = 0
    for judgement in judgements:
        try:
            # The _id is now diary_number + actual_judgement_date
            collection.update_one({'_id': judgement['_id']}, {'$set': judgement}, upsert=True)
            saved_count += 1
        except pymongo.errors.DuplicateKeyError: 
            logger.warning(f"Duplicate entry skipped for _id: {judgement['_id']}.")
        except Exception as e:
            logger.error(f"Error saving judgement with _id {judgement.get('_id', 'N/A')} to MongoDB: {e}", exc_info=True)
    logger.info(f"Attempted to save {len(judgements)} judgements, {saved_count} were newly inserted/updated.")
    return saved_count


# --- Main Scraper Logic ---
def scrape_sci_judgements_monthly():
    logger.info("Starting SCI Judgements Monthly Scraper.")
    driver = setup_driver()
    judgements_collection, processed_months_coll = connect_to_mongodb()

    # Define the range of years and months to scrape
    # Example: Scrape from Jan 2024 to May 2025 (inclusive of start_month, exclusive of end_month if using range)
    start_year = 1961
    start_month_num = 6
    
    # Scrape backwards for N months
    num_months_to_scrape_backward = 12 * 50 # Scrape for 50 years (600 months)

    current_target_date = datetime.date(start_year, start_month_num, 1)

    for _ in range(num_months_to_scrape_backward):
        target_year = current_target_date.year
        target_month = current_target_date.month

        month_year_str_for_tracking = f"{target_year:04d}-{target_month:02d}" # YYYY-MM for tracking
        month_year_str_for_query = f"{target_month:02d}-{target_year:04d}"    # MM-YYYY for query/logging

        logger.info(f"--- Processing month: {month_year_str_for_query} ({month_year_str_for_tracking}) ---")

        if is_month_processed(month_year_str_for_tracking, processed_months_coll):
            logger.info(f"Month {month_year_str_for_tracking} has already been processed. Skipping.")
            # Move to the previous month
            current_target_date = (current_target_date.replace(day=1) - datetime.timedelta(days=1)).replace(day=1)
            continue

        # Calculate From Date (1st of month) and To Date (last day of month)
        from_date_obj = datetime.date(target_year, target_month, 1)
        last_day = get_last_day_of_month(target_year, target_month)
        to_date_obj = datetime.date(target_year, target_month, last_day)

        from_date_str_for_form = from_date_obj.strftime("%d-%m-%Y")
        to_date_str_for_form = to_date_obj.strftime("%d-%m-%Y")
        
        logger.info(f"Querying from: {from_date_str_for_form} to: {to_date_str_for_form}")

        # --- Form filling and submission logic (similar to daily, but with month range) ---
        try:
            driver.get(WEBSITE_URL)
            WebDriverWait(driver, 20).until(EC.presence_of_element_located((By.ID, "from_date")))
        except Exception as page_load_err:
            logger.error(f"Critical error loading page for month {month_year_str_for_query}: {page_load_err}. Skipping month.", exc_info=True)
            mark_month_as_processed(month_year_str_for_tracking, processed_months_coll)
            current_target_date = (current_target_date.replace(day=1) - datetime.timedelta(days=1)).replace(day=1)
            continue
        
        max_attempts_per_month = 10
        attempts_for_month = 0
        submitted_successfully_for_month = False

        while attempts_for_month < max_attempts_per_month and not submitted_successfully_for_month:
            attempts_for_month += 1
            logger.info(f"Form attempt {attempts_for_month}/{max_attempts_per_month} for month: {month_year_str_for_query}")
            
            try:
                # Fill From Date
                from_date_input = driver.find_element(By.ID, "from_date")
                from_date_input.clear(); from_date_input.click(); time.sleep(0.2)
                from_date_numeric = from_date_obj.strftime("%d%m%Y")
                for char_val in from_date_numeric: from_date_input.send_keys(char_val); time.sleep(0.08)
                driver.find_element(By.TAG_NAME, 'body').click(); time.sleep(0.3)
                logger.info(f"Filled From Date: {from_date_input.get_attribute('value')}")

                # Fill To Date
                to_date_input = driver.find_element(By.ID, "to_date")
                to_date_input.clear(); to_date_input.click(); time.sleep(0.2)
                to_date_numeric = to_date_obj.strftime("%d%m%Y")
                for char_val in to_date_numeric: to_date_input.send_keys(char_val); time.sleep(0.08)
                driver.find_element(By.TAG_NAME, 'body').click(); time.sleep(0.3)
                logger.info(f"Filled To Date: {to_date_input.get_attribute('value')}")
                time.sleep(0.5)

                # CAPTCHA logic (largely same as before)
                captcha_input_field = driver.find_element(By.ID, "siwp_captcha_value_0")
                captcha_image_element = WebDriverWait(driver, 10).until(EC.visibility_of_element_located((By.ID, "siwp_captcha_image_0")))
                driver.execute_script("arguments[0].scrollIntoView({block: 'center', inline: 'center'});", captcha_image_element); time.sleep(0.8)
                
                captcha_image_bytes = None
                try: captcha_image_bytes = captcha_image_element.screenshot_as_png
                except Exception as e_scr: logger.warning(f"Element screenshot failed: {e_scr}. Fallback needed if this persists.")
                
                # Fallback CAPTCHA image capture (if primary fails)
                if not captcha_image_bytes:
                    logger.warning("Primary CAPTCHA screenshot failed. Fallback screenshot method needs robust implementation if this occurs frequently.")
                    # Add robust fallback cropping logic here if element.screenshot_as_png is unreliable
                    # For now, if primary fails, we might fail this attempt for CAPTCHA.
                    if attempts_for_month < max_attempts_per_month: driver.get(WEBSITE_URL); time.sleep(3)
                    continue 

                if captcha_image_bytes:
                    debug_img_path = f"DEBUG_captcha_{month_year_str_for_tracking.replace('-', '_')}_{attempts_for_month}.png"
                    with open(debug_img_path, "wb") as f_debug: f_debug.write(captcha_image_bytes)
                    logger.info(f"CAPTCHA image saved for debug: {debug_img_path}")

                    captcha_solution = solve_captcha_with_gemini(captcha_image_bytes)
                    if captcha_solution:
                        captcha_input_field.clear(); captcha_input_field.send_keys(captcha_solution)
                        driver.find_element(By.NAME, "submit").click()
                        logger.info("Form submitted for month.")

                        try: 
                            WebDriverWait(driver, 7).until(EC.presence_of_element_located((By.XPATH, "//div[@class='notfound' and contains(text(), 'captcha code entered was incorrect')]")))
                            logger.warning("CAPTCHA incorrect for month query.")
                            # ... (refresh/reload logic for next attempt)
                            if attempts_for_month < max_attempts_per_month:
                                try: driver.find_element(By.CLASS_NAME, "captcha-refresh-btn").click(); time.sleep(2.5)
                                except: driver.get(WEBSITE_URL); time.sleep(3)
                        except TimeoutException: 
                            logger.info("CAPTCHA potentially correct for month query, checking results.")
                            try:
                                WebDriverWait(driver, 45).until( # Longer wait for monthly results
                                    EC.any_of(
                                        EC.presence_of_element_located((By.XPATH, "//div[@class='distTableContent']//table//tbody//tr[1]")),
                                        EC.presence_of_element_located((By.XPATH, "//div[contains(text(), 'No records found')]"))
                                    )
                                )
                                html_content = driver.page_source
                                if "No records found" in html_content:
                                    logger.info(f"No records found for month: {month_year_str_for_query}.")
                                else:
                                    judgements = parse_judgement_data(html_content, month_year_str_for_query) # Pass MM-YYYY
                                    if judgements:
                                        num_saved = save_judgements_to_mongodb(judgements, judgements_collection)
                                        logger.info(f"Month {month_year_str_for_query}: Found {len(judgements)} judgements, saved/updated {num_saved}.")
                                    else:
                                        logger.warning(f"No judgements parsed for {month_year_str_for_query} though 'No records' not found.")
                                        # Save HTML for debugging this case
                                        with open(f"DEBUG_empty_parse_month_{month_year_str_for_tracking.replace('-', '_')}.html", "w", encoding="utf-8") as f_html:
                                            f_html.write(html_content)
                                submitted_successfully_for_month = True
                                mark_month_as_processed(month_year_str_for_tracking, processed_months_coll)
                            except TimeoutException:
                                logger.error(f"Timeout waiting for results (month: {month_year_str_for_query}). Saving page source/screenshot.")
                                driver.save_screenshot(f"TIMEOUT_RESULTS_MONTH_{month_year_str_for_tracking.replace('-', '_')}.png")
                                with open(f"TIMEOUT_RESULTS_MONTH_{month_year_str_for_tracking.replace('-', '_')}.html", "w", encoding="utf-8") as f_html: f_html.write(driver.page_source)
                                if attempts_for_month < max_attempts_per_month: driver.get(WEBSITE_URL); time.sleep(3)
                            # ... (other exception handling for results processing)
                    else: # Gemini fail
                        logger.warning("Failed to get CAPTCHA solution from Gemini (monthly).")
                        if attempts_for_month < max_attempts_per_month:
                            try: driver.find_element(By.CLASS_NAME, "captcha-refresh-btn").click(); time.sleep(2.5)
                            except: driver.get(WEBSITE_URL); time.sleep(3)
                else: # CAPTCHA image capture fail
                    logger.error("Failed to capture CAPTCHA image bytes (monthly).")
                    if attempts_for_month < max_attempts_per_month: driver.get(WEBSITE_URL); time.sleep(3)
            except Exception as e_attempt:
                logger.error(f"Error during attempt {attempts_for_month} for month {month_year_str_for_query}: {e_attempt}", exc_info=True)
                # ... (error handling, screenshot, reload for next attempt)
                try: driver.save_screenshot(f"ERROR_ATTEMPT_MONTH_{month_year_str_for_tracking.replace('-', '_')}_{attempts_for_month}.png")
                except: pass
                if attempts_for_month < max_attempts_per_month: 
                    try: driver.get(WEBSITE_URL); time.sleep(3)
                    except: break # Critical error on reload
        
        if not submitted_successfully_for_month:
            logger.error(f"Failed to process month {month_year_str_for_query} after {max_attempts_per_month} attempts. Marking as processed to skip.")
            mark_month_as_processed(month_year_str_for_tracking, processed_months_coll)

        # Move to the previous month for the next iteration
        current_target_date = (current_target_date.replace(day=1) - datetime.timedelta(days=1)).replace(day=1)
        time.sleep(5) # Politeness delay between months

    logger.info("--- Monthly Scraping finished ---")
    driver.quit()

if __name__ == "__main__":
    try:
        # Check for placeholder API keys is now inside get_gemini_api_key
        scrape_sci_judgements_monthly() # Call the new monthly function
    except Exception as e_main:
        logger.critical(f"A critical error occurred in the main execution: {e_main}", exc_info=True)
    finally:
        logger.info("Monthly scraper script execution ended.")
