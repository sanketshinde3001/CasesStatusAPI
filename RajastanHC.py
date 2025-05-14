import os
import time
import datetime
import io
import re # For Gemini solution extraction
import logging # For logging
import base64 # For decoding base64 CAPTCHA
# Removed PIL import as it's not strictly needed anymore for captcha solving with Gemini
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service as ChromeService
from selenium.webdriver.support.ui import WebDriverWait, Select
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, NoSuchElementException, ElementNotInteractableException, StaleElementReferenceException, WebDriverException
from webdriver_manager.chrome import ChromeDriverManager
from bs4 import BeautifulSoup # Still useful for small HTML snippets within cells
import pymongo
import google.generativeai as genai

# --- Logging Setup ---
LOG_DIR = "logs"
CAPTCHA_DEBUG_DIR = "captcha_debug_images"
os.makedirs(LOG_DIR, exist_ok=True)
os.makedirs(CAPTCHA_DEBUG_DIR, exist_ok=True)

log_formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(module)s:%(lineno)d - %(message)s')
log_file = os.path.join(LOG_DIR, 'rhc_jodhpur_daily_by_category_direct_links.log') # Updated log file name

file_handler = logging.FileHandler(log_file, mode='a')
file_handler.setFormatter(log_formatter)
file_handler.setLevel(logging.INFO)

console_handler = logging.StreamHandler()
console_handler.setFormatter(log_formatter)
console_handler.setLevel(logging.INFO)

logger = logging.getLogger()
logger.setLevel(logging.INFO)
if logger.hasHandlers():
    logger.handlers.clear()
logger.addHandler(file_handler)
logger.addHandler(console_handler)

# --- Configuration ---
WEBSITE_URL = "https://hcraj.nic.in/cishcraj-jdp/JudgementFilters/#"
BASE_PDF_URL_VIEWER_PREFIX = "http://hcraj.nic.in/cishcraj-jdp/pdfjs-dist/web/viewer.php?file="
DIRECT_PDF_STORE_BASE = "https://hcraj.nic.in/cishcraj-jdp/storefiles/createordjud/"

GEMINI_API_KEYS = [
    "AIzaSyDtPJcsbsviS5RJqqtAoxFcal6SVhsb6u8",
    "AIzaSyD3edZdPrM8k7wb_qtAYrvFH8sPkMeFJyk",
    "AIzaSyAHciFxpwCZfOJ4xziNM6Et4m8ksbUoJIQ",
    "AIzaSyAH7_pM86jSjbKLymOuMtI-eG0v4TqvO0M",
    "AIzaSyCqf3KDSx3TY7zkP8GS1QslCHmjyYnhkvo",
    "AIzaSyCrsvLI8pZIQW5gGVCR1PHSJyFGEULxmXs"
]
MONGODB_CONNECTION_STRING = "mongodb+srv://sanketshinde3123:n2rXcex2EDQPk96G@scrapper.fqsbnrc.mongodb.net/?retryWrites=true&w=majority&appName=scrapper"
MONGODB_DATABASE_NAME = "rhc_judgements"
MONGODB_COLLECTION_NAME = "judgements_jodhpur_daily_cat_links" # Keeping name consistent
PROCESSED_DATE_CATEGORIES_COLLECTION_NAME = "processed_date_categories_jodhpur_links" # Keeping name consistent

current_gemini_api_key_index = 0

CATEGORIES_TO_SCRAPE = {
    "2": "Criminal",
    "1": "Civil",
    "3": "Writ"
}

# --- Helper Functions (Gemini, Driver Setup, MongoDB Connect - Largely Unchanged) ---

def get_gemini_api_key():
    global current_gemini_api_key_index
    if not GEMINI_API_KEYS or any(key.startswith("YOUR_GEMINI_API_KEY") for key in GEMINI_API_KEYS):
        logger.critical("Gemini API keys are not configured properly. Please replace placeholders.")
        raise ValueError("Gemini API keys are not configured properly.")
    key = GEMINI_API_KEYS[current_gemini_api_key_index]
    current_gemini_api_key_index = (current_gemini_api_key_index + 1) % len(GEMINI_API_KEYS)
    return key

def configure_gemini():
    api_key = get_gemini_api_key()
    genai.configure(api_key=api_key)
    logger.info(f"Configured Gemini with API key ending: ...{api_key[-4:]}")

def solve_captcha_with_gemini(image_bytes):
    if not image_bytes:
        logger.warning("No image bytes provided to Gemini solver.")
        return None
    try:
        configure_gemini()
        model = genai.GenerativeModel('gemini-2.0-flash') # Or 'gemini-pro-vision'
        img_part = {"mime_type": "image/png", "data": image_bytes}
        prompt = "Extract the alphanumeric characters from this CAPTCHA image precisely. Provide only the sequence of characters you see, with no extra text or explanation."
        logger.info("Sending CAPTCHA to Gemini...")
        response = model.generate_content([prompt, img_part],
                                           generation_config=genai.types.GenerationConfig(temperature=0.1))

        if response.parts:
            solution = response.text.strip()
            logger.info(f"Gemini raw solution: '{solution}'")
            cleaned_solution = re.sub(r'[^a-zA-Z0-9]', '', solution)
            if len(cleaned_solution) == 6:
                logger.info(f"Gemini extracted 6-char alphanumeric solution: {cleaned_solution}")
                return cleaned_solution
            else:
                logger.warning(f"Gemini solution '{cleaned_solution}' (from '{solution}') is not 6 characters long.")
                # Save problematic CAPTCHA + response for analysis
                try:
                    timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
                    img_path = os.path.join(CAPTCHA_DEBUG_DIR, f"FAILED_LEN_CAPTCHA_{timestamp}.png")
                    txt_path = os.path.join(CAPTCHA_DEBUG_DIR, f"FAILED_LEN_RESPONSE_{timestamp}.txt")
                    with open(img_path, "wb") as f_img: f_img.write(image_bytes)
                    with open(txt_path, "w") as f_txt: f_txt.write(f"Raw: '{solution}'\nCleaned: '{cleaned_solution}'")
                    logger.info(f"Saved failed length CAPTCHA image and response for review.")
                except Exception as e_save: logger.error(f"Could not save failed length CAPTCHA debug files: {e_save}")
                return None
        else:
            logger.warning(f"Gemini returned no parts in response. Full Response: {response}")
            if hasattr(response, 'prompt_feedback') and response.prompt_feedback.block_reason:
                logger.warning(f"Gemini content blocked. Reason: {response.prompt_feedback.block_reason_message}")
            return None
    except Exception as e:
        logger.error(f"Error solving CAPTCHA with Gemini: {e}", exc_info=True)
        return None

def setup_driver():
    options = webdriver.ChromeOptions()
    options.add_argument("--start-maximized")
    # options.add_argument("--headless") # Keep headless commented out initially
    options.add_argument("--disable-gpu")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_experimental_option('excludeSwitches', ['enable-logging'])
    options.add_argument("user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/90.0.4430.212 Safari/537.36")
    options.add_experimental_option("excludeSwitches", ["enable-automation"])
    options.add_experimental_option('useAutomationExtension', False)
    options.add_argument('--log-level=3') # Suppress console logs

    service = ChromeService(ChromeDriverManager().install())
    driver = webdriver.Chrome(service=service, options=options)
    logger.info("WebDriver setup complete.")
    return driver

def connect_to_mongodb():
    client = pymongo.MongoClient(MONGODB_CONNECTION_STRING)
    db = client[MONGODB_DATABASE_NAME]
    judgements_collection = db[MONGODB_COLLECTION_NAME]
    processed_dates_coll = db[PROCESSED_DATE_CATEGORIES_COLLECTION_NAME]

    judgements_collection.create_index(
        [("case_number", pymongo.ASCENDING),
         ("order_judgement_date", pymongo.ASCENDING),
         ("category_value", pymongo.ASCENDING)],
        name="case_date_category_idx", unique=True, sparse=True
    )
    processed_dates_coll.create_index(
        [("date_str", pymongo.ASCENDING), ("category_value", pymongo.ASCENDING)],
        name="date_category_idx", unique=True
    )
    logger.info(f"Connected to MongoDB. DB: {MONGODB_DATABASE_NAME}, Collections: {MONGODB_COLLECTION_NAME}, {PROCESSED_DATE_CATEGORIES_COLLECTION_NAME}")
    return judgements_collection, processed_dates_coll

# --- Modified Database Tracking Functions (Unchanged) ---

def is_date_category_processed(date_str, category_value, processed_dates_coll):
    """Checks if a specific date and category combination has been processed."""
    return processed_dates_coll.count_documents({"date_str": date_str, "category_value": category_value}) > 0

def mark_date_category_as_processed(date_str, category_value, category_name, status, processed_dates_coll, details=None):
    """Marks a specific date and category combination as processed with a status."""
    doc = {
        "date_str": date_str,
        "category_value": category_value,
        "category_name": category_name,
        "status": status,
        "processed_at": datetime.datetime.utcnow()
    }
    if details:
        doc["details"] = details
    try:
        processed_dates_coll.update_one(
            {"date_str": date_str, "category_value": category_value},
            {"$set": doc},
            upsert=True
        )
        logger.info(f"Marked date {date_str}, category '{category_name}' as processed with status: {status}.")
    except Exception as e:
        logger.error(f"Error marking date {date_str}, category '{category_name}' as processed: {e}", exc_info=True)

# --- Modified Parsing Function (Unchanged) ---

def parse_rajasthan_hc_judgement_data(driver, search_query_date_str, category_value, category_name):
    """Parses judgement data, constructing PDF URLs directly from button attributes."""
    logger.info(f"Starting direct parsing for date: {search_query_date_str}, Category: {category_name}")
    judgements = []

    try:
        table_element = WebDriverWait(driver, 15).until(
            EC.presence_of_element_located((By.ID, "sample_1"))
        )
        tbody_element = table_element.find_element(By.TAG_NAME, "tbody")
        row_elements = tbody_element.find_elements(By.TAG_NAME, "tr")

        if len(row_elements) == 1:
            try:
                single_cell = row_elements[0].find_element(By.TAG_NAME, "td")
                if "No matching records found" in single_cell.text:
                    logger.info(f"Table shows 'No matching records found' for {search_query_date_str}, Category: {category_name}.")
                    return judgements
            except NoSuchElementException:
                logger.warning(f"Single row found for {search_query_date_str}, Cat: {category_name}, but no 'td' cell.")
                return judgements

    except (TimeoutException, NoSuchElementException) as e:
        logger.warning(f"Results table or tbody not found for date {search_query_date_str}, Category: {category_name}. Error: {e}")
        debug_html_path = os.path.join(CAPTCHA_DEBUG_DIR, f"DEBUG_no_table_{search_query_date_str.replace('/', '_')}_{category_name}.html")
        try:
            with open(debug_html_path, "w", encoding="utf-8") as f_debug_html: f_debug_html.write(driver.page_source)
            logger.info(f"Saved HTML for debugging missing table: {debug_html_path}")
        except Exception as e_save_html: logger.error(f"Failed to save debug HTML: {e_save_html}")
        return judgements

    logger.info(f"Found {len(row_elements)} row WebElements in the table for date: {search_query_date_str}, Category: {category_name}")
    if not row_elements: return judgements

    parsed_judgements_count = 0

    for row_idx, row_element in enumerate(row_elements):
        try:
            cell_elements = row_element.find_elements(By.TAG_NAME, "td")
        except StaleElementReferenceException:
            logger.warning(f"Row {row_idx+1} became stale for {search_query_date_str}, Cat: {category_name}. Re-finding rows.")
            try:
                table_element = driver.find_element(By.ID, "sample_1")
                tbody_element = table_element.find_element(By.TAG_NAME, "tbody")
                all_current_rows = tbody_element.find_elements(By.TAG_NAME, "tr")
                if row_idx < len(all_current_rows):
                    row_element = all_current_rows[row_idx]
                    cell_elements = row_element.find_elements(By.TAG_NAME, "td")
                    logger.info(f"Successfully re-acquired row {row_idx+1}.")
                else:
                    logger.error(f"Could not re-find row at index {row_idx} after staleness. Skipping row.")
                    continue
            except Exception as e_refind:
                logger.error(f"Error re-finding rows after stale element exception: {e_refind}. Skipping row.")
                continue
        except Exception as e_find_cells:
             logger.error(f"Error finding cells in row {row_idx+1} for {search_query_date_str}, Cat: {category_name}: {e_find_cells}. Skipping row.")
             continue

        try:
            if len(cell_elements) < 5:
                if len(cell_elements) == 1 and "No matching records found" in cell_elements[0].text.strip():
                    continue
                logger.warning(f"Skipping row {row_idx+1} ({search_query_date_str}, Cat: {category_name}): Found {len(cell_elements)} cells (expected 5+). Text: '{row_element.text[:100]}...'")
                continue

            sr_no = cell_elements[0].text.strip()
            case_details_cell_html = cell_elements[1].get_attribute('innerHTML')
            parts_from_br = [part.strip() for part in re.split('<br\s*/?>', case_details_cell_html, flags=re.IGNORECASE)]
            case_number_str = BeautifulSoup(parts_from_br[0], 'html.parser').get_text(strip=True) if len(parts_from_br) > 0 else "N/A"
            petitioner_respondent_str = BeautifulSoup(parts_from_br[1], 'html.parser').get_text(strip=True) if len(parts_from_br) > 1 else "N/A"
            honble_justice_str = cell_elements[2].text.strip()
            order_judgement_date_str = cell_elements[3].text.strip()
            action_details = []
            action_cell_element = cell_elements[4]
            view_pdf_url = None
            view_params = {}

            try:
                view_button_selenium = action_cell_element.find_element(By.XPATH, ".//button[contains(@onclick, \"DownloadOrdJud(this,'V')\")]")
                case_no_attr = view_button_selenium.get_attribute('data-caseno')
                order_no_attr = view_button_selenium.get_attribute('data-orderno')
                for attr_name in view_button_selenium.get_property('attributes'):
                    if attr_name['name'].startswith('data-'):
                        view_params[attr_name['name'].replace('data-', '')] = attr_name['value']

                if case_no_attr and order_no_attr:
                    direct_pdf_link = f"{DIRECT_PDF_STORE_BASE}{case_no_attr}_{order_no_attr}.pdf"
                    viewer_link = f"{BASE_PDF_URL_VIEWER_PREFIX}{direct_pdf_link}"
                    view_pdf_url = viewer_link
                    logger.debug(f"Constructed View URL (Row {row_idx+1}): {view_pdf_url}")
                    action_details.append({
                        "type": "view",
                        "url": view_pdf_url,
                        "direct_pdf_url": direct_pdf_link,
                        "parameters": view_params
                    })
                else:
                    logger.warning(f"Missing data-caseno or data-orderno for view button on row {row_idx+1}. Cannot construct URL.")
                    action_details.append({
                        "type": "view_error",
                        "error_message": "Missing data-caseno or data-orderno attribute",
                        "parameters": view_params
                    })
            except NoSuchElementException:
                logger.debug(f"No view button found via Selenium for row {row_idx+1}")
                action_details.append({"type": "view_error", "error_message": "View button not found"})
            except Exception as e_view:
                logger.error(f"Error processing view button attributes for row {row_idx+1}: {e_view}", exc_info=True)
                action_details.append({
                    "type": "view_error",
                    "error_message": f"Generic error processing view button: {str(e_view)}",
                    "parameters": view_params
                })

            try:
                download_button_selenium = action_cell_element.find_element(By.XPATH, ".//button[contains(@onclick, \"DownloadOrdJud(this,'D')\")]")
                download_params = {}
                for attr_name in download_button_selenium.get_property('attributes'):
                    if attr_name['name'].startswith('data-'):
                        download_params[attr_name['name'].replace('data-', '')] = attr_name['value']
                dl_case_no = download_params.get('caseno')
                dl_order_no = download_params.get('orderno')
                direct_download_url = None
                if dl_case_no and dl_order_no:
                    direct_download_url = f"{DIRECT_PDF_STORE_BASE}{dl_case_no}_{dl_order_no}.pdf"
                action_details.append({
                    "type": "download_info",
                    "parameters": download_params,
                    "direct_download_url": direct_download_url
                })
            except NoSuchElementException:
                logger.debug(f"No download button info found via Selenium for row {row_idx+1}")
            except Exception as e_dl_info:
                 logger.error(f"Error getting download_info for row {row_idx+1}: {e_dl_info}")

            try:
                try:
                    dt_obj = datetime.datetime.strptime(order_judgement_date_str, "%d-%b-%Y")
                    formatted_date_for_db = dt_obj.strftime("%Y-%m-%d")
                except ValueError:
                    logger.warning(f"Could not parse date '{order_judgement_date_str}'. Storing as original string.")
                    formatted_date_for_db = order_judgement_date_str

                unique_id_str = f"{case_number_str.replace('/', '-')}_{formatted_date_for_db}_{category_value}"

                judgement_detail = {
                    "_id": unique_id_str,
                    "serial_number": sr_no,
                    "case_number": case_number_str,
                    "petitioner_respondent": petitioner_respondent_str,
                    "honble_justice": honble_justice_str,
                    "order_judgement_date": order_judgement_date_str,
                    "order_judgement_date_iso": formatted_date_for_db,
                    "category_value": category_value,
                    "category_name": category_name,
                    "action_details": action_details,
                    "view_pdf_url": view_pdf_url,
                    "search_query_date": search_query_date_str,
                    "scraped_at_utc": datetime.datetime.utcnow()
                }
                if action_details:
                     view_action = next((item for item in action_details if item.get('type') == 'view' and 'direct_pdf_url' in item), None)
                     if view_action:
                         judgement_detail["direct_pdf_url"] = view_action.get("direct_pdf_url")

                judgements.append(judgement_detail)
                parsed_judgements_count += 1
            except Exception as e_construct:
                 logger.error(f"Error constructing judgement dict for row {row_idx+1} ({search_query_date_str}, Cat: {category_name}): {e_construct}", exc_info=True)
                 continue
        except StaleElementReferenceException:
             logger.warning(f"Inner StaleElementReferenceException caught processing row {row_idx+1}. Skipping.")
             continue
        except Exception as e_row_parse:
            logger.error(f"Outer error processing row {row_idx+1} ({search_query_date_str}, Cat: {category_name}): {e_row_parse}", exc_info=True)
            try: logger.error(f"Problematic row HTML: {row_element.get_attribute('outerHTML')[:500]}")
            except: pass
            continue

    logger.info(f"Finished parsing. Extracted {parsed_judgements_count} judgements for date: {search_query_date_str}, Category: {category_name}")
    return judgements

# --- Saving Function (Unchanged) ---

def save_judgements_to_mongodb(judgements, collection, date_str, category_name):
    """Saves judgements to MongoDB, logging context."""
    if not judgements:
        logger.info(f"No judgements to save for {date_str}, Category: {category_name}.")
        return 0
    saved_count = 0
    skipped_duplicates = 0
    error_count = 0
    for judgement in judgements:
        try:
            result = collection.update_one(
                {'_id': judgement['_id']},
                {'$set': judgement},
                upsert=True
            )
            if result.upserted_id:
                saved_count += 1
            # else: logger.debug(f"Updated existing judgement: {judgement['_id']}")
        except pymongo.errors.DuplicateKeyError:
            logger.warning(f"Duplicate entry skipped for _id: {judgement['_id']} (unexpected with upsert).")
            skipped_duplicates += 1
        except Exception as e:
            logger.error(f"Error saving judgement _id {judgement.get('_id', 'N/A')} ({date_str}, Cat: {category_name}): {e}", exc_info=True)
            error_count += 1

    log_message = f"Date {date_str}, Category {category_name}: Attempted {len(judgements)} judgements. "
    if saved_count > 0: log_message += f"{saved_count} new. "
    if skipped_duplicates > 0: log_message += f"{skipped_duplicates} duplicate errors. "
    if error_count > 0: log_message += f"{error_count} save errors. "
    logger.info(log_message.strip())
    return saved_count

# --- Main Scraper Logic (MODIFIED) ---

def scrape_rajasthan_hc_daily():
    logger.info("Starting RHC Jodhpur Scraper by Category (Direct URL Construction).")
    driver = setup_driver()
    judgements_collection, processed_dates_coll = connect_to_mongodb()

    num_days_to_scrape_backward = 30
    start_date = datetime.date(2025, 5, 12) # Adjust start date

    # <<< MODIFICATION: Load initial page ONCE >>>
    try:
        logger.info(f"Loading initial page: {WEBSITE_URL}")
        driver.get(WEBSITE_URL)
        WebDriverWait(driver, 30).until(EC.presence_of_element_located((By.ID, "partyFromDate")))
        logger.info("Initial page loaded successfully.")
        time.sleep(2) # Allow page elements to settle
    except (TimeoutException, WebDriverException) as e:
        logger.critical(f"Failed to load initial page: {e}. Exiting.")
        if driver: driver.quit()
        return

    for i in range(num_days_to_scrape_backward):
        current_target_date = start_date - datetime.timedelta(days=i)
        target_date_str_for_form = current_target_date.strftime("%d/%m/%Y")
        target_date_str_for_tracking = current_target_date.strftime("%Y-%m-%d")

        logger.info(f"--- Processing Date: {target_date_str_for_form} ({target_date_str_for_tracking}) ---")

        for category_value, category_name in CATEGORIES_TO_SCRAPE.items():
            logger.info(f"--- --- Processing Category: {category_name} ({category_value}) for Date: {target_date_str_for_form} --- ---")

            if is_date_category_processed(target_date_str_for_tracking, category_value, processed_dates_coll):
                logger.info(f"Date {target_date_str_for_tracking}, Category '{category_name}' already processed. Skipping.")
                continue

            max_attempts_per_category = 10
            attempts_for_category = 0
            processed_successfully_for_category = False
            form_needs_reset = True # Flag to indicate if form needs filling (true for first attempt)

            while attempts_for_category < max_attempts_per_category and not processed_successfully_for_category:
                attempts_for_category += 1
                logger.info(f"Attempt {attempts_for_category}/{max_attempts_per_category} for Date: {target_date_str_for_form}, Category: {category_name}")

                try:
                    # <<< MODIFICATION: Remove driver.get() from here >>>
                    # logger.info(f"Loading fresh page ({WEBSITE_URL}) for attempt {attempts_for_category}") # Removed
                    # driver.get(WEBSITE_URL) # Removed
                    # WebDriverWait(driver, 25).until(EC.presence_of_element_located((By.ID, "partyFromDate"))) # Already loaded or should be after 'Back'
                    # time.sleep(2) # Pause after potential 'Back' click or on retry

                    # --- Fill Form (only if needed) ---
                    if form_needs_reset:
                        logger.info("Ensuring form elements are ready...")
                        from_date_input = WebDriverWait(driver, 15).until(EC.element_to_be_clickable((By.ID, "partyFromDate")))
                        # driver.execute_script("arguments[0].scrollIntoView(true);", from_date_input) # Less scrolling might be better if already visible
                        logger.info("Clearing and filling 'From Date'")
                        from_date_input.clear()
                        # Use JavaScript to set value directly, often more reliable
                        driver.execute_script(f"arguments[0].value='{target_date_str_for_form}';", from_date_input)
                        # for char_val in target_date_str_for_form: from_date_input.send_keys(char_val); time.sleep(0.08) # Slower, maybe less reliable
                        # driver.find_element(By.TAG_NAME, 'body').click(); time.sleep(0.5) # Trigger blur/update events if needed

                        to_date_input = driver.find_element(By.ID, "partyToDate")
                        logger.info("Clearing and filling 'To Date'")
                        to_date_input.clear()
                        driver.execute_script(f"arguments[0].value='{target_date_str_for_form}';", to_date_input)
                        # for char_val in target_date_str_for_form: to_date_input.send_keys(char_val); time.sleep(0.08)
                        # driver.find_element(By.TAG_NAME, 'body').click(); time.sleep(0.5)

                        logger.info(f"Filled Dates: From='{from_date_input.get_attribute('value')}', To='{to_date_input.get_attribute('value')}'")
                        if from_date_input.get_attribute('value') != target_date_str_for_form or to_date_input.get_attribute('value') != target_date_str_for_form:
                            logger.warning("Date fields did not fill correctly via JS, trying send_keys...")
                            from_date_input.clear()
                            for char_val in target_date_str_for_form: from_date_input.send_keys(char_val); time.sleep(0.08)
                            to_date_input.clear()
                            for char_val in target_date_str_for_form: to_date_input.send_keys(char_val); time.sleep(0.08)
                            driver.find_element(By.TAG_NAME, 'body').click(); time.sleep(0.5) # Try blur again
                            logger.info(f"Re-Filled Dates: From='{from_date_input.get_attribute('value')}', To='{to_date_input.get_attribute('value')}'")


                        category_dropdown = Select(WebDriverWait(driver, 10).until(EC.element_to_be_clickable((By.ID, "casebasetype"))))
                        category_dropdown.select_by_value(category_value)
                        logger.info(f"Selected Category: {category_name} ({category_value})")
                        time.sleep(1)
                        form_needs_reset = False # Form is now set for this category until success/failure or new category

                    # --- Handle CAPTCHA ---
                    captcha_input_field = driver.find_element(By.ID, "txtCaptcha")
                    captcha_image_element = WebDriverWait(driver, 15).until(EC.visibility_of_element_located((By.ID, "captcha")))
                    # driver.execute_script("arguments[0].scrollIntoView({behavior: 'auto', block: 'center', inline: 'nearest'});", captcha_image_element) # Less scrolling
                    time.sleep(1) # Wait for image

                    base64_image_data = captcha_image_element.get_attribute("src")
                    if not base64_image_data or not base64_image_data.startswith("data:image/png;base64,"):
                        logger.error("CAPTCHA src not base64. Attempting refresh/retry.")
                        try:
                            # Try clicking the refresh button if available
                            refresh_button = driver.find_element(By.XPATH, "//img[contains(@onclick, 'refreshcaptcha')]") # Adjust XPath if needed
                            driver.execute_script("arguments[0].click();", refresh_button)
                            logger.info("Clicked CAPTCHA refresh.")
                            time.sleep(3)
                        except NoSuchElementException:
                            logger.warning("CAPTCHA refresh button not found.")
                            time.sleep(3)
                        continue # Retry the attempt

                    captcha_image_bytes = base64.b64decode(base64_image_data.split(",")[1])
                    if not captcha_image_bytes:
                        logger.error("Failed to decode CAPTCHA. Retrying attempt."); time.sleep(3); continue

                    debug_img_path = os.path.join(CAPTCHA_DEBUG_DIR, f"CAPTCHA_{target_date_str_for_tracking.replace('-', '_')}_Cat{category_value}_Attempt{attempts_for_category}.png")
                    try:
                        with open(debug_img_path, "wb") as f_debug: f_debug.write(captcha_image_bytes)
                    except Exception as e_save_cap: logger.error(f"Failed to save CAPTCHA image: {e_save_cap}")

                    captcha_solution = solve_captcha_with_gemini(captcha_image_bytes)
                    if captcha_solution:
                        captcha_input_field.clear(); captcha_input_field.send_keys(captcha_solution); time.sleep(0.5)

                        # --- Submit Form ---
                        search_button = WebDriverWait(driver, 10).until(EC.element_to_be_clickable((By.ID, "btncasedetail1_1")))
                        driver.execute_script("arguments[0].click();", search_button)
                        logger.info(f"Form submitted for Date: {target_date_str_for_form}, Category: {category_name}")

                        # --- Wait for and Analyze Outcome ---
                        try:
                            # <<< MODIFICATION: Add new alert div locators to the wait condition >>>
                            outcome_locator = EC.any_of(
                                EC.presence_of_element_located((By.ID, "sample_1")), # Results table
                                EC.visibility_of_element_located((By.XPATH, "//div[contains(@class, 'alert-danger') and contains(text(), 'No Record Found !!')]")), # NEW no records div
                                EC.visibility_of_element_located((By.XPATH, "//div[contains(@class, 'alert-danger') and contains(text(), 'Invalid Security Code !!')]")), # NEW invalid captcha div
                                EC.visibility_of_element_located((By.XPATH, "//span[@id='ErrorMsgCaptcha' and normalize-space(text())!='']")), # Original captcha span
                                EC.visibility_of_element_located((By.XPATH, "//div[contains(@class, 'note-common') or contains(@class, 'alert-info')][normalize-space(.)='No record found.']")), # Original no records div
                                EC.visibility_of_element_located((By.XPATH, "//b[@class='myjudcountmsg'][contains(., 'Search results are more than 1000')]")) # >1000 warning
                            )
                            logger.info("Waiting for response after submit...")
                            # Add a slightly longer timeout as page processing might take time
                            WebDriverWait(driver, 45).until(outcome_locator)
                            logger.info("Page response received after submit.")
                            time.sleep(1) # Small pause to let elements stabilize

                            # <<< MODIFICATION: Check new conditions first >>>
                            # 1. Check NEW Invalid Security Code Div
                            try:
                                invalid_code_div = driver.find_element(By.XPATH, "//div[contains(@class, 'alert-danger') and contains(text(), 'Invalid Security Code !!')]")
                                if invalid_code_div.is_displayed():
                                    logger.warning(f"CAPTCHA INCORRECT (Alert Div): '{invalid_code_div.text.strip()}'. Retrying attempt.")
                                    # Don't reset form here, just need new CAPTCHA
                                    form_needs_reset = False # Keep form data
                                    time.sleep(2); continue # Continue to next attempt (retry)
                            except NoSuchElementException: pass

                            # 2. Check Original CAPTCHA Error Span (as fallback)
                            try:
                                captcha_error_span = driver.find_element(By.XPATH, "//span[@id='ErrorMsgCaptcha' and normalize-space(text())!='']")
                                if captcha_error_span.is_displayed():
                                    logger.warning(f"CAPTCHA INCORRECT (Span): '{captcha_error_span.text.strip()}'. Retrying attempt.")
                                    form_needs_reset = False # Keep form data
                                    time.sleep(2); continue # Continue to next attempt (retry)
                            except NoSuchElementException: pass

                            # 3. Check NEW No Record Found Div
                            try:
                                no_records_div_new = driver.find_element(By.XPATH, "//div[contains(@class, 'alert-danger') and contains(text(), 'No Record Found !!')]")
                                if no_records_div_new.is_displayed():
                                    logger.info(f"NO RECORDS FOUND (Alert Div) for {target_date_str_for_form}, Cat: {category_name}.")
                                    mark_date_category_as_processed(target_date_str_for_tracking, category_value, category_name, "NO_RECORDS", processed_dates_coll)
                                    processed_successfully_for_category = True; break # Break attempt loop
                            except NoSuchElementException: pass

                            # If already successful from step 3, skip further checks
                            if processed_successfully_for_category: continue

                            # 4. Check >1000 Warning
                            try:
                                over_1000_warning = driver.find_element(By.XPATH, "//b[@class='myjudcountmsg'][contains(., 'Search results are more than 1000')]")
                                if over_1000_warning.is_displayed():
                                    logger.warning(f"RESULT LIMIT EXCEEDED (>1000) for {target_date_str_for_form}, Cat: {category_name}. Cannot scrape fully.")
                                    mark_date_category_as_processed(target_date_str_for_tracking, category_value, category_name, "FAILED_OVER_1000", processed_dates_coll)
                                    processed_successfully_for_category = True; break # Break attempt loop
                            except NoSuchElementException: pass

                             # If already successful from step 4, skip further checks
                            if processed_successfully_for_category: continue

                            # 5. Check Original No Records Div (as fallback)
                            try:
                                no_records_div_old = driver.find_element(By.XPATH, "//div[contains(@class, 'note-common') or contains(@class, 'alert-info')][normalize-space(.)='No record found.']")
                                if no_records_div_old.is_displayed():
                                    logger.info(f"NO RECORDS FOUND (note-common/alert-info Div) for {target_date_str_for_form}, Cat: {category_name}.")
                                    mark_date_category_as_processed(target_date_str_for_tracking, category_value, category_name, "NO_RECORDS", processed_dates_coll)
                                    processed_successfully_for_category = True; break # Break attempt loop
                            except NoSuchElementException: pass

                            # If already successful from step 5, skip further checks
                            if processed_successfully_for_category: continue

                            # 6. Expect Results Table (if none of the above matched)
                            logger.info("No errors/warnings/no-record messages detected, expecting results table...")
                            try:
                                # Confirm table presence explicitly again
                                WebDriverWait(driver, 10).until(EC.presence_of_element_located((By.ID, "sample_1")))
                                logger.info("Results table (id='sample_1') confirmed present.")

                                # --- Select "All" Records ---
                                try:
                                    select_all_start_time = time.time()
                                    select_records_dropdown_element = WebDriverWait(driver, 10).until(
                                        EC.element_to_be_clickable((By.NAME, "sample_1_length"))
                                    )
                                    select_records_dropdown = Select(select_records_dropdown_element)
                                    current_value = select_records_dropdown.first_selected_option.get_attribute("value")
                                    if current_value != "-1":
                                        logger.info("Attempting to select 'All' entries per page.")
                                        select_records_dropdown.select_by_value("-1")
                                        logger.info("Selected 'All' from dropdown. Waiting for table update...")
                                        WebDriverWait(driver, 35).until( # Increased wait
                                           EC.invisibility_of_element_located((By.ID, "sample_1_processing"))
                                        )
                                        logger.info("'Processing' indicator gone. Table update likely complete. (Wait took {:.1f}s)".format(time.time() - select_all_start_time))
                                        time.sleep(3) # Extra pause for safety
                                except TimeoutException:
                                    logger.warning("Timeout finding/interacting with 'records per page' dropdown. Proceeding with current view.")
                                except NoSuchElementException:
                                     logger.warning("Could not find 'records per page' dropdown. Proceeding with current view.")
                                except Exception as e_select_all:
                                    logger.error(f"Error during 'Select All': {e_select_all}. Proceeding.", exc_info=True)

                                # --- Parse and Save Data ---
                                judgements = parse_rajasthan_hc_judgement_data(driver, target_date_str_for_form, category_value, category_name)
                                if judgements:
                                    save_judgements_to_mongodb(judgements, judgements_collection, target_date_str_for_form, category_name)
                                else:
                                    try: # Check for 'No matching records' within an empty table body
                                        empty_msg = driver.find_element(By.XPATH, "//td[@class='dataTables_empty' and contains(., 'No matching records found')]")
                                        if empty_msg.is_displayed():
                                            logger.info(f"Parser found no judgements, confirmed 'No matching records' in table body for {target_date_str_for_form}, Cat: {category_name}.")
                                        else:
                                             logger.warning(f"Parser found no judgements, but 'No matching records' message not found in table body for {target_date_str_for_form}, Cat: {category_name}.")
                                    except NoSuchElementException:
                                         logger.warning(f"Parser found no judgements, and 'dataTables_empty' cell not found for {target_date_str_for_form}, Cat: {category_name}. Potential parsing issue.")

                                mark_date_category_as_processed(target_date_str_for_tracking, category_value, category_name, "SUCCESS", processed_dates_coll, details=f"Parsed {len(judgements)} records")
                                processed_successfully_for_category = True; break # Break attempt loop

                            except TimeoutException:
                                logger.error(f"Timeout waiting for final results table confirmation after submit for {target_date_str_for_form}, Cat: {category_name}. Attempt {attempts_for_category}")
                                driver.save_screenshot(os.path.join(CAPTCHA_DEBUG_DIR, f"TIMEOUT_RESULTS_FINAL_{target_date_str_for_tracking.replace('-', '_')}_{category_name}_Attempt{attempts_for_category}.png"))
                                # Let attempt loop continue or fail naturally
                                if attempts_for_category == max_attempts_per_category:
                                    mark_date_category_as_processed(target_date_str_for_tracking, category_value, category_name, "ERROR", processed_dates_coll, details="Timeout waiting for results table")

                        except TimeoutException:
                            logger.error(f"Overall Timeout waiting for ANY response after submit for {target_date_str_for_form}, Cat: {category_name}. Attempt {attempts_for_category}")
                            driver.save_screenshot(os.path.join(CAPTCHA_DEBUG_DIR, f"TIMEOUT_ANY_RESPONSE_{target_date_str_for_tracking.replace('-', '_')}_{category_name}_Attempt{attempts_for_category}.png"))
                            # Let attempt loop continue or fail naturally
                            if attempts_for_category == max_attempts_per_category:
                                 mark_date_category_as_processed(target_date_str_for_tracking, category_value, category_name, "ERROR", processed_dates_coll, details="Timeout waiting for any page response")

                    else: # CAPTCHA solution failed
                        logger.warning(f"Failed Gemini CAPTCHA for {target_date_str_for_form}, Cat: {category_name}. Attempt {attempts_for_category}. Retrying.")
                        form_needs_reset = False # Keep form data, just retry CAPTCHA
                        time.sleep(3) # Wait before retry

                # --- General Exception Handling for Attempt ---
                except (TimeoutException, ElementNotInteractableException, StaleElementReferenceException, WebDriverException) as e_interact:
                     logger.error(f"Interaction/Stale/Timeout Error during attempt {attempts_for_category} ({target_date_str_for_form}, Cat: {category_name}): {type(e_interact).__name__} - {str(e_interact)[:200]}", exc_info=False) # Less verbose logging for common errors
                     form_needs_reset = True # Assume form state is uncertain, reset on next attempt
                     try: driver.save_screenshot(os.path.join(CAPTCHA_DEBUG_DIR, f"ERROR_INTERACT_{target_date_str_for_tracking.replace('-', '_')}_{category_name}_Attempt{attempts_for_category}.png"))
                     except: pass
                     if isinstance(e_interact, WebDriverException) and "net::ERR_INTERNET_DISCONNECTED" in str(e_interact): logger.error("Network error. Pausing..."); time.sleep(60)
                     else: time.sleep(5) # General pause before retry
                     if attempts_for_category >= max_attempts_per_category:
                        mark_date_category_as_processed(target_date_str_for_tracking, category_value, category_name, "ERROR", processed_dates_coll, details=f"{type(e_interact).__name__} on last attempt"); break
                except Exception as e_attempt:
                    logger.error(f"General Error, Attempt {attempts_for_category} ({target_date_str_for_form}, Cat: {category_name}): {e_attempt}", exc_info=True)
                    form_needs_reset = True # Assume form state is uncertain
                    try: driver.save_screenshot(os.path.join(CAPTCHA_DEBUG_DIR, f"ERROR_GENERAL_{target_date_str_for_tracking.replace('-', '_')}_{category_name}_Attempt{attempts_for_category}.png"))
                    except: pass
                    if attempts_for_category >= max_attempts_per_category:
                        mark_date_category_as_processed(target_date_str_for_tracking, category_value, category_name, "ERROR", processed_dates_coll, details=f"General error: {str(e_attempt)[:100]}"); break
                    time.sleep(5)

            # --- After attempt loop finishes for a category ---
            if not processed_successfully_for_category:
                 # Check if it was already marked failed (e.g., >1000)
                 status_check = processed_dates_coll.find_one({"date_str": target_date_str_for_tracking, "category_value": category_value})
                 if not status_check or status_check.get("status") not in ["FAILED_OVER_1000", "NO_RECORDS", "ERROR"]: # Only mark as ERROR if not already marked failed/no records
                    logger.error(f"Failed to process Date {target_date_str_for_form}, Category {category_name} after {max_attempts_per_category} attempts. Marking as ERROR.")
                    mark_date_category_as_processed(target_date_str_for_tracking, category_value, category_name, "ERROR", processed_dates_coll, details=f"Failed after {max_attempts_per_category} attempts")
                 else:
                     logger.info(f"Category {category_name} for {target_date_str_for_form} already marked as {status_check.get('status')}. Not updating status.")

            # <<< MODIFICATION: Click "Back" button to return to form view, if processing finished (success or expected failure) >>>
            # Only click back if we successfully processed, found no records, or hit the >1000 limit. Avoid clicking if it failed due to errors.
            # We also need to check if the back button is actually present (it only appears after a search)
            if processed_successfully_for_category: # Includes SUCCESS, NO_RECORDS, FAILED_OVER_1000
                logger.info(f"Processing for category {category_name} finished. Attempting to click 'Back' button.")
                try:
                    # Wait for the back button container to be visible first
                    WebDriverWait(driver, 15).until(EC.visibility_of_element_located((By.ID, "srchbackBtndiv")))
                    back_button = WebDriverWait(driver, 10).until(
                        EC.element_to_be_clickable((By.XPATH, "//div[@id='srchbackBtndiv']/button[contains(@class, 'btn-danger')]"))
                    )
                    driver.execute_script("arguments[0].click();", back_button)
                    logger.info("Clicked 'Back' button successfully. Waiting for form to reappear...")
                    # Wait for a known element of the form to be clickable again
                    WebDriverWait(driver, 20).until(EC.element_to_be_clickable((By.ID, "partyFromDate")))
                    logger.info("Form view confirmed after clicking 'Back'.")
                    form_needs_reset = True # Ensure next category fills the form
                    time.sleep(3) # Extra pause after going back
                except TimeoutException:
                    logger.error("Timeout waiting for or clicking the 'Back' button or waiting for form after click. Attempting full page reload as fallback.")
                    driver.get(WEBSITE_URL) # Fallback to reload
                    WebDriverWait(driver, 25).until(EC.presence_of_element_located((By.ID, "partyFromDate")))
                    form_needs_reset = True
                except Exception as e_back:
                    logger.error(f"Error clicking 'Back' button: {e_back}. Attempting full page reload as fallback.", exc_info=True)
                    driver.get(WEBSITE_URL) # Fallback to reload
                    WebDriverWait(driver, 25).until(EC.presence_of_element_located((By.ID, "partyFromDate")))
                    form_needs_reset = True
            else:
                # If the category failed after max attempts due to errors, a full reload might be safer
                logger.warning(f"Category {category_name} processing failed after max attempts. Forcing page reload before next category.")
                try:
                    driver.get(WEBSITE_URL)
                    WebDriverWait(driver, 25).until(EC.presence_of_element_located((By.ID, "partyFromDate")))
                    form_needs_reset = True
                except Exception as e_reload_fail:
                     logger.error(f"Failed to reload page after error: {e_reload_fail}. Scraper may be unstable.")
                     # Attempt to continue, but it might fail on the next category


            logger.info(f"--- --- Finished processing Category: {category_name} for Date: {target_date_str_for_form} --- ---")
            # time.sleep(5) # Delay between categories (already have sleep after Back/Reload)

        logger.info(f"--- Finished ALL Categories for Date: {target_date_str_for_form} ---")
        time.sleep(5) # Shorter delay between dates if needed, Back button logic adds pauses

    logger.info("--- Daily Scraping by Category (Direct URL) finished ---")
    if driver:
        driver.quit()
        logger.info("WebDriver closed.")

if __name__ == "__main__":
    try:
        if any(not key or key.startswith("YOUR_GEMINI") for key in GEMINI_API_KEYS):
             print("ERROR: Invalid Gemini API key detected. Update GEMINI_API_KEYS.")
             logger.critical("Invalid Gemini API key detected. Update GEMINI_API_KEYS.")
        else:
            scrape_rajasthan_hc_daily()
    except ValueError as ve:
        print(f"Configuration Error: {ve}")
        logger.critical(f"Configuration Error: {ve}")
    except KeyboardInterrupt:
        logger.warning("Script interrupted by user.")
    except Exception as e_main:
        logger.critical(f"Critical error in main execution: {e_main}", exc_info=True)
    finally:
        logger.info("RHC Jodhpur scraper (direct URL) script execution ended.")
