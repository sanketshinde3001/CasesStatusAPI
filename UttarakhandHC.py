import os
import time
import datetime
import io
import re # For Gemini solution extraction
import logging # For logging
import calendar # For getting last day of month (not actively used in chunking logic but kept)
from urllib.parse import urljoin # To build absolute URLs
from PIL import Image
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service as ChromeService
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, NoSuchElementException, ElementNotInteractableException
from webdriver_manager.chrome import ChromeDriverManager
from bs4 import BeautifulSoup
import pymongo
import google.generativeai as genai
import tempfile
import shutil
import atexit
import uuid # Added for generating unique IDs

# --- Logging Setup ---
log_formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(name)s:%(lineno)d - %(message)s')
log_file = 'hc_tripura_scraper_chunked.log'

root_logger = logging.getLogger()
if root_logger.hasHandlers():
    root_logger.handlers.clear()

file_handler = logging.FileHandler(log_file, mode='a', encoding='utf-8')
file_handler.setFormatter(log_formatter)
file_handler.setLevel(logging.INFO)

console_handler = logging.StreamHandler()
console_handler.setFormatter(log_formatter)
console_handler.setLevel(logging.INFO)

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
logger.addHandler(file_handler)
logger.addHandler(console_handler)
logger.propagate = False

# --- Configuration ---
WEBSITE_URL = "https://hcservices.ecourts.gov.in/ecourtindiaHC/cases/s_orderdate.php?state_cd=15&dist_cd=1&court_code=1&stateNm=Uttarakhand"
BASE_URL = "https://hcservices.ecourts.gov.in/ecourtindiaHC/cases/"

GEMINI_API_KEYS = [
    "AIzaSyDJtRHN2IPw34SWYGccykHPgQPsLIs-fWw", # Replace with your key
    "AIzaSyCMvwHi_wq_e6CH3M2Ueya_QwYzQUQxw6A"  # Replace with your key
]
MONGODB_CONNECTION_STRING = "mongodb+srv://sanketshinde3123:n2rXcex2EDQPk96G@scrapper.fqsbnrc.mongodb.net/?retryWrites=true&w=majority&appName=scrapper" # Replace with your key
MONGODB_DATABASE_NAME = "high_court_scrap"
MONGODB_COLLECTION_NAME = "uttarakhand_data_scrap"
PROCESSED_DATES_COLLECTION_NAME = "processed_dates_uttarakhand"

current_gemini_api_key_index = 0
MAX_GEMINI_RETRY = 5
DATA_CHUNK_SIZE = 5 # Can increase this to scrape more days at once, but be do not exceed 6-7 days to avoid timeout issues

# --- Helper Functions ---
def get_gemini_api_key():
    global current_gemini_api_key_index
    valid_keys = [k for k in GEMINI_API_KEYS if k and not k.startswith("YOUR_GEMINI_API_KEY") and len(k) > 15]
    if not valid_keys:
        logger.critical("No valid Gemini API keys found. Please check GEMINI_API_KEYS configuration.")
        raise ValueError("Gemini API keys are not configured properly.")
    key_to_use_index = current_gemini_api_key_index % len(valid_keys)
    key = valid_keys[key_to_use_index]
    current_gemini_api_key_index += 1
    logger.info(f"Using Gemini API key ending: ...{key[-4:]} (Index {key_to_use_index})")
    return key

def configure_gemini():
    try:
        api_key = get_gemini_api_key()
        genai.configure(api_key=api_key)
        logger.debug(f"Configured Gemini with API key ending: ...{api_key[-4:]}")
        return True
    except ValueError as e:
         logger.critical(f"Gemini config failed due to key issue: {e}")
         return False
    except Exception as e:
        logger.error(f"Error configuring Gemini: {e}", exc_info=True)
        return False

def solve_captcha_with_gemini(image_bytes):
    if not image_bytes:
        logger.warning("No image bytes for Gemini.")
        return None
    for attempt in range(MAX_GEMINI_RETRY):
        if not configure_gemini(): return None
        try:
            logger.info(f"Sending CAPTCHA to Gemini (SDK Attempt {attempt + 1}/{MAX_GEMINI_RETRY})...")
            model = genai.GenerativeModel('gemini-2.0-flash')
            img_part = {"mime_type": "image/png", "data": image_bytes}
            prompt = ("Return ONLY the text of the CAPTCHA image. No explanation or extra words. The CAPTCHA consists of alphanumeric characters.")
            response = model.generate_content([prompt, img_part],
                                              generation_config=genai.types.GenerationConfig(
                                              candidate_count=1, max_output_tokens=20, temperature=0.1))
            if response.parts:
                solution = re.sub(r'[^A-Za-z0-9]', '', response.text.strip())
                logger.info(f"Gemini raw: '{response.text.strip()}', Cleaned: '{solution}'")
                if solution and 3 <= len(solution) <= 7:
                    return solution
                else:
                    logger.warning(f"Gemini invalid solution: '{solution}' (length/content mismatch)")
            else:
                safety = response.prompt_feedback if hasattr(response, 'prompt_feedback') else 'N/A'
                logger.warning(f"Gemini no parts. Safety: {safety}. Resp: {response}")
        except Exception as e:
            logger.error(f"Gemini CAPTCHA error (SDK Attempt {attempt + 1}): {e}", exc_info=True)
        if attempt < MAX_GEMINI_RETRY - 1:
            logger.info("Waiting 2s before Gemini retry...")
            time.sleep(2)
    logger.error("Failed Gemini CAPTCHA solve after retries.")
    return None

def setup_driver():
    temp_dir = None
    try:
        options = webdriver.ChromeOptions()
        options.add_argument("--start-maximized")
        # options.add_argument("--headless")
        options.add_argument("--disable-gpu")
        options.add_argument("--no-sandbox")
        options.add_argument("--disable-dev-shm-usage")
        options.add_argument('--log-level=3')
        options.add_experimental_option('excludeSwitches', ['enable-logging'])
        options.add_experimental_option("prefs", {"profile.default_content_setting_values.notifications": 2})
        temp_dir = tempfile.mkdtemp(prefix="chrome_temp_")
        options.add_argument(f"--user-data-dir={temp_dir}")
        logger.info(f"Chrome user data dir: {temp_dir}")
        def cleanup_temp_dir():
            if temp_dir and os.path.exists(temp_dir):
                try: shutil.rmtree(temp_dir, ignore_errors=True); logger.info(f"Cleaned temp dir: {temp_dir}")
                except Exception as e: logger.error(f"Error cleaning temp dir {temp_dir}: {e}")
        atexit.register(cleanup_temp_dir)
        service = ChromeService(ChromeDriverManager().install())
        driver = webdriver.Chrome(service=service, options=options)
        logger.info("WebDriver setup complete.")
        return driver
    except Exception as e:
        logger.error(f"WebDriver setup failed: {e}", exc_info=True)
        if temp_dir and os.path.exists(temp_dir): shutil.rmtree(temp_dir, ignore_errors=True)
        raise

def connect_to_mongodb():
    try:
        client = pymongo.MongoClient(MONGODB_CONNECTION_STRING, serverSelectionTimeoutMS=10000)
        client.admin.command('ping')
        logger.info("MongoDB connected.")
        db = client[MONGODB_DATABASE_NAME]
        judgements_coll = db[MONGODB_COLLECTION_NAME]
        processed_dates_coll = db[PROCESSED_DATES_COLLECTION_NAME]
        # Index on case_number and date, but NOT unique, to allow duplicates of this combination
        # sparse=True means the index only includes documents that have the indexed fields.
        judgements_coll.create_index([("case_number", 1), ("date", 1)], sparse=True)
        processed_dates_coll.create_index("date_str", unique=True)
        logger.info(f"DB: '{MONGODB_DATABASE_NAME}', Collections: '{MONGODB_COLLECTION_NAME}', '{PROCESSED_DATES_COLLECTION_NAME}'")
        return judgements_coll, processed_dates_coll
    except Exception as e:
        logger.critical(f"MongoDB setup failed: {e}", exc_info=True)
        raise

def is_date_processed(date_str_yyyymmdd, coll):
    try: return coll.count_documents({"date_str": date_str_yyyymmdd}) > 0
    except Exception as e: logger.error(f"DB check error for {date_str_yyyymmdd}: {e}"); return False

def mark_date_as_processed(date_str_yyyymmdd, coll):
    try:
        coll.update_one({"date_str": date_str_yyyymmdd}, {"$set": {"processed_at": datetime.datetime.utcnow()}}, upsert=True)
        logger.info(f"Marked date {date_str_yyyymmdd} as processed.")
    except Exception as e: logger.error(f"DB mark error for {date_str_yyyymmdd}: {e}")

def parse_judgement_data(html_content, search_date_from_str_ddmmyyyy, search_date_to_str_ddmmyyyy):
    logger.info(f"Parsing judgement data for search date range: {search_date_from_str_ddmmyyyy} to {search_date_to_str_ddmmyyyy}")
    soup = BeautifulSoup(html_content, 'html.parser')
    judgements = []
    results_tbody = soup.find('tbody', id='showList1')

    if not results_tbody:
        logger.warning(f"Results tbody with id 'showList1' not found for range {search_date_from_str_ddmmyyyy} to {search_date_to_str_ddmmyyyy}.")
        # ... (debug HTML saving code remains the same)
        return judgements

    rows = results_tbody.find_all('tr')
    logger.info(f"Found {len(rows)} data rows in tbody#showList1 for range {search_date_from_str_ddmmyyyy} to {search_date_to_str_ddmmyyyy}.")
    if not rows: return judgements

    parsed_count = 0
    for row_idx, row in enumerate(rows, start=1):
        cells = row.find_all('td')
        if len(cells) < 4:
             logger.warning(f"Row {row_idx} (Range: {search_date_from_str_ddmmyyyy}-{search_date_to_str_ddmmyyyy}): Skip, {len(cells)} cells, expect 4. Content: {row.get_text(strip=True, separator='|')}")
             continue
        try:
            full_case_identifier = cells[1].get_text(strip=True)

            case_type_parsed = None
            case_year_parsed = None

            if full_case_identifier:
                parts = full_case_identifier.split('/')
                if len(parts) >= 1:
                    case_type_parsed = parts[0].strip()
                if len(parts) == 3:
                    case_year_parsed = parts[2].strip()
                    if not (parts[1].strip().isdigit() and \
                            case_year_parsed.isdigit() and \
                            len(case_year_parsed) == 4 and \
                            1900 < int(case_year_parsed) < 2100):
                        logger.warning(f"Row {row_idx} (Range: {search_date_from_str_ddmmyyyy}-{search_date_to_str_ddmmyyyy}): Parsed parts for '{full_case_identifier}' (Type: {case_type_parsed}, Year: {case_year_parsed}) seem unusual.")
                elif len(parts) == 2:
                    if parts[1].strip().isdigit() and len(parts[1].strip()) == 4:
                        case_year_parsed = parts[1].strip()
                    logger.warning(f"Row {row_idx} (Range: {search_date_from_str_ddmmyyyy}-{search_date_to_str_ddmmyyyy}): Case identifier '{full_case_identifier}' has 2 parts. Parsed Type: {case_type_parsed}, Year: {case_year_parsed}.")
                else:
                    logger.warning(f"Row {row_idx} (Range: {search_date_from_str_ddmmyyyy}-{search_date_to_str_ddmmyyyy}): Case identifier '{full_case_identifier}' does not have 2 or 3 parts.")
            else:
                logger.warning(f"Row {row_idx} (Range: {search_date_from_str_ddmmyyyy}-{search_date_to_str_ddmmyyyy}): Case identifier string is empty.")

            date_tag = cells[2].find('h2', class_='h2class')
            judgement_date_str = date_tag.get_text(strip=True) if date_tag else None
            if not judgement_date_str or not re.fullmatch(r'\d{2}-\d{2}-\d{4}', judgement_date_str):
                logger.warning(f"Row {row_idx} (Range: {search_date_from_str_ddmmyyyy}-{search_date_to_str_ddmmyyyy}): Invalid date ('{judgement_date_str}'). Skip.")
                continue

            link_tag = cells[3].find('a', href=True)
            judgement_links_dict = {}
            if link_tag:
                pdf_rel_url = link_tag.get('href', '').strip()
                pdf_text = link_tag.get_text(strip=True) or "OrderLink"
                if pdf_rel_url:
                    pdf_abs_url = urljoin(BASE_URL, pdf_rel_url)
                    judgement_links_dict[pdf_text] = pdf_abs_url
                else:
                    judgement_links_dict[pdf_text] = None
            
            # Generate a unique _id by appending a UUID
            # This ensures every parsed item gets a new DB entry, allowing "duplicates" of content
            unique_id_val = f"TRP_{full_case_identifier}_{judgement_date_str}_{uuid.uuid4()}"

            doc = {
                "_id": unique_id_val,
                "date": judgement_date_str,
                "case_type": case_type_parsed,
                "case_year": case_year_parsed,
                "case_number": full_case_identifier,
                "judgement_links": judgement_links_dict
            }
            judgements.append(doc)
            parsed_count += 1
            logger.debug(f"Parsed row {row_idx}: Case={full_case_identifier}, Date={judgement_date_str} (_id ends with ...{str(unique_id_val)[-36:]})")
        except Exception as e:
            logger.error(f"Error parsing row {row_idx} (Range: {search_date_from_str_ddmmyyyy}-{search_date_to_str_ddmmyyyy}): {e}", exc_info=True)
            # ... (problematic row HTML logging remains the same)
    logger.info(f"Finished parsing range {search_date_from_str_ddmmyyyy}-{search_date_to_str_ddmmyyyy}. Extracted {parsed_count} judgements.")
    return judgements

def save_judgements_to_mongodb(judgements, collection):
    if not judgements: logger.debug("No judgements to save."); return 0
    inserted_count = 0
    failed_count = 0
    for doc in judgements:
        if not doc.get('_id'): # Should always have _id due to new generation logic
            logger.error(f"Doc missing _id (should not happen): {doc.get('case_number', 'Unknown')}")
            failed_count +=1
            continue
        try:
            collection.insert_one(doc)
            inserted_count += 1
        except Exception as e:
            logger.error(f"DB insert error for _id {doc['_id']}: {e}")
            failed_count += 1
    logger.info(f"DB: Attempted to insert={len(judgements)}, Successfully Inserted={inserted_count}, Failed={failed_count}.")
    return inserted_count # Return number of successful inserts

# --- Main Scraper Logic (Chunked) ---
# ... (scrape_hc_judgements_chunked function remains largely the same,
#      as the core changes are in parsing and saving) ...
# --- Main Scraper Logic (Chunked) ---
def scrape_hc_judgements_chunked():
    logger.info(f"Starting Chunked Scraper for Tripura High Court (Chunk Size: {DATA_CHUNK_SIZE} days).")
    driver = None
    try:
        driver = setup_driver()
        judgements_coll, processed_dates_coll = connect_to_mongodb()
    except Exception as setup_err:
        logger.critical(f"Setup failed: {setup_err}. Exiting.", exc_info=True); return

    overall_start_date_obj = datetime.date.today()
    num_total_days_to_scrape_backward = 50000 # Total days to go back

    logger.info(f"Starting scrape from {overall_start_date_obj:%Y-%m-%d} back {num_total_days_to_scrape_backward} days, in {DATA_CHUNK_SIZE}-day chunks.")

    # Iterate in chunks
    for day_offset_start_of_chunk in range(0, num_total_days_to_scrape_backward, DATA_CHUNK_SIZE):
        chunk_end_date_obj = overall_start_date_obj - datetime.timedelta(days=day_offset_start_of_chunk)
        chunk_start_date_obj = overall_start_date_obj - datetime.timedelta(days=min(
            day_offset_start_of_chunk + DATA_CHUNK_SIZE - 1,
            num_total_days_to_scrape_backward - 1 
        ))
        if chunk_start_date_obj > chunk_end_date_obj: 
            logger.warning(f"Calculated chunk_start_date {chunk_start_date_obj} is after chunk_end_date {chunk_end_date_obj}. Skipping.")
            continue

        form_from_date_str = chunk_start_date_obj.strftime("%d-%m-%Y")
        form_to_date_str = chunk_end_date_obj.strftime("%d-%m-%Y")
        
        date_range_display_str = f"{form_from_date_str} to {form_to_date_str}"
        logger.info(f"--- Processing Date Range: {date_range_display_str} (Offset {day_offset_start_of_chunk}) ---")

        all_dates_in_chunk_processed = True
        dates_in_current_chunk_yyyymmdd = [] 
        
        current_iter_date_for_check = chunk_start_date_obj
        while current_iter_date_for_check <= chunk_end_date_obj:
            date_track_str = current_iter_date_for_check.strftime("%Y-%m-%d")
            dates_in_current_chunk_yyyymmdd.append(date_track_str)
            if not is_date_processed(date_track_str, processed_dates_coll):
                all_dates_in_chunk_processed = False
            current_iter_date_for_check += datetime.timedelta(days=1)
        
        if not dates_in_current_chunk_yyyymmdd:
            logger.warning(f"No dates generated for chunk: {date_range_display_str}. Skipping.")
            continue

        if all_dates_in_chunk_processed:
            logger.info(f"Date range {date_range_display_str} already fully processed. Skipping.")
            continue

        try:
            logger.debug(f"Loading {WEBSITE_URL}")
            driver.get(WEBSITE_URL)
            WebDriverWait(driver, 30).until(EC.presence_of_element_located((By.ID, "from_date")))
        except Exception as e:
            logger.error(f"Page load failed for range {date_range_display_str}: {e}. Skipping chunk.", exc_info=True)
            time.sleep(5); continue

        max_attempts_per_chunk = 5
        attempt_no = 0
        success_this_range = False
        data_processed_this_range = False

        while attempt_no < max_attempts_per_chunk and not success_this_range:
            attempt_no += 1
            logger.info(f"Attempt {attempt_no}/{max_attempts_per_chunk} for date range: {date_range_display_str}")

            try:
                wait = WebDriverWait(driver, 15)
                logger.debug(f"Filling dates: From='{form_from_date_str}', To='{form_to_date_str}'")
                from_date_elem = wait.until(EC.element_to_be_clickable((By.ID, "from_date")))
                driver.execute_script(f"arguments[0].value = '{form_from_date_str}';", from_date_elem)
                to_date_elem = wait.until(EC.element_to_be_clickable((By.ID, "to_date")))
                driver.execute_script(f"arguments[0].value = '{form_to_date_str}';", to_date_elem)
                time.sleep(0.5)
                logger.info(f"Dates filled: From='{from_date_elem.get_attribute('value')}', To='{to_date_elem.get_attribute('value')}'")

                time.sleep(1)
                cap_input = wait.until(EC.visibility_of_element_located((By.ID, "captcha")))
                cap_img = wait.until(EC.visibility_of_element_located((By.ID, "captcha_image")))
                driver.execute_script("arguments[0].scrollIntoView({block: 'center'});", cap_img); time.sleep(1.5)
                img_bytes = cap_img.screenshot_as_png
                if not img_bytes: logger.error("CAPTCHA screenshot failed."); continue

                cap_solution = solve_captcha_with_gemini(img_bytes)
                if not cap_solution:
                    logger.warning("Gemini failed for CAPTCHA, reloading for next attempt in chunk.")
                    if attempt_no < max_attempts_per_chunk: driver.get(WEBSITE_URL); time.sleep(3)
                    continue

                logger.info(f"Attempting CAPTCHA solution: {cap_solution}")
                cap_input.clear(); cap_input.send_keys(cap_solution); time.sleep(0.5)

                submit_btn_sel = "input[type='button'][name='submit1'][value='Go']"
                logger.debug(f"Clicking submit: {submit_btn_sel}")
                try:
                    submit = wait.until(EC.element_to_be_clickable((By.CSS_SELECTOR, submit_btn_sel)))
                    submit.click(); logger.info("Form submitted for range.")
                except Exception as e_click:
                    logger.error(f"Submit click error for range {date_range_display_str}: {e_click}", exc_info=True)
                    if attempt_no < max_attempts_per_chunk: driver.get(WEBSITE_URL); time.sleep(3)
                    continue

                results_wait_timeout = 60 
                final_state = "timeout" 

                results_locator = (By.XPATH, "//tbody[@id='showList1']/tr[1]/td[1]")
                no_records_locator = (By.XPATH, "//h2[@class='h2class' and contains(text(), 'Record Not Found')]")
                invalid_captcha_locator = (By.ID, "txtmsg")
                invalid_captcha_error_state_locator = (By.XPATH, "//input[@id='txtmsg' and (normalize-space(@value)='Invalid Captcha' or normalize-space(@title)='Invalid Captcha')]")

                try:
                    logger.debug(f"Waiting for page state (max {results_wait_timeout}s) for range {date_range_display_str}...")
                    WebDriverWait(driver, results_wait_timeout).until(
                        EC.any_of(
                            EC.presence_of_element_located(invalid_captcha_error_state_locator),
                            EC.presence_of_element_located(no_records_locator),
                            EC.presence_of_element_located(results_locator)
                        )
                    )
                    logger.debug("EC.any_of condition met. Determining actual state by re-checking elements...")

                    try:
                        invalid_captcha_element = driver.find_element(*invalid_captcha_locator)
                        txtmsg_value = invalid_captcha_element.get_attribute('value')
                        txtmsg_title = invalid_captcha_element.get_attribute('title')
                        if invalid_captcha_element.is_displayed() and \
                           (txtmsg_value == 'Invalid Captcha' or txtmsg_title == 'Invalid Captcha'):
                            final_state = "invalid_captcha"
                    except NoSuchElementException: pass
                    except Exception as e_recheck_captcha: logger.warning(f"Error during invalid_captcha re-check: {e_recheck_captcha}")

                    if final_state == "timeout":
                        try:
                            if driver.find_element(*no_records_locator).is_displayed(): final_state = "no_records"
                        except NoSuchElementException: pass

                    if final_state == "timeout":
                        try:
                            if driver.find_element(*results_locator).is_displayed(): final_state = "results_found"
                        except NoSuchElementException: final_state = "unknown_after_any_of"
                    
                    logger.info(f"Page state for range {date_range_display_str} determined as: {final_state}")

                except TimeoutException:
                    logger.error(f"Timeout waiting for ANY known page state for range {date_range_display_str}. Saving debug.")
                    ts = time.strftime("%Y%m%d_%H%M%S")
                    fname_base = f"TIMEOUT_AnyState_{form_from_date_str.replace('-','_')}_to_{form_to_date_str.replace('-','_')}_att{attempt_no}_{ts}"
                    try:
                        driver.save_screenshot(f"{fname_base}.png"); logger.info(f"Saved {fname_base}.png")
                        with open(f"{fname_base}.html", "w", encoding="utf-8") as f: f.write(driver.page_source); logger.info(f"Saved {fname_base}.html")
                    except Exception as e_save: logger.error(f"Failed saving timeout debug: {e_save}")
                    if attempt_no < max_attempts_per_chunk: logger.info("Reloading page."); driver.get(WEBSITE_URL); time.sleep(3)
                    continue

                page_html = driver.page_source

                if final_state == "invalid_captcha":
                    logger.warning(f"Invalid CAPTCHA (Attempt {attempt_no}) for range {date_range_display_str}. Solution: '{cap_solution}'")
                    if attempt_no < max_attempts_per_chunk: driver.get(WEBSITE_URL); time.sleep(3)
                    continue

                elif final_state == "no_records":
                    logger.info(f"Record Not Found confirmed for range {date_range_display_str}.")
                    success_this_range = True
                    data_processed_this_range = True 
                    break

                elif final_state == "results_found":
                    logger.info(f"Results state confirmed for range {date_range_display_str}. Parsing...")
                    judgements = parse_judgement_data(page_html, form_from_date_str, form_to_date_str)
                    if judgements:
                        save_judgements_to_mongodb(judgements, judgements_coll)
                    else:
                        logger.warning(f"No judgements parsed for range {date_range_display_str} despite 'results_found' state.")
                    data_processed_this_range = True
                    success_this_range = True
                    break
                
                elif final_state == "unknown_after_any_of" or final_state == "timeout":
                     logger.error(f"State '{final_state}' for range {date_range_display_str}. HTML saved for review.")
                     ts = time.strftime("%Y%m%d_%H%M%S")
                     debug_fname = f"DEBUG_UnknownOrTimeoutFinalState_{final_state}_{form_from_date_str.replace('-','_')}_to_{form_to_date_str.replace('-','_')}_att{attempt_no}_{ts}.html"
                     try:
                         with open(debug_fname, "w", encoding="utf-8") as f: f.write(page_html); logger.info(f"Saved HTML: {debug_fname}")
                     except Exception as e_w: logger.error(f"Failed saving HTML: {e_w}")

                     logger.warning(f"Attempting to parse HTML for state '{final_state}' (range {date_range_display_str}) as a last resort...")
                     judgements = parse_judgement_data(page_html, form_from_date_str, form_to_date_str)
                     if judgements:
                         logger.info(f"LAST RESORT: Parsed {len(judgements)} judgements for state '{final_state}' (range {date_range_display_str}).")
                         save_judgements_to_mongodb(judgements, judgements_coll)
                         data_processed_this_range = True
                         success_this_range = True
                         break
                     else:
                         logger.error(f"LAST RESORT: Still no judgements parsed for state '{final_state}' (range {date_range_display_str}).")
                         if attempt_no < max_attempts_per_chunk: logger.info("Reloading page."); driver.get(WEBSITE_URL); time.sleep(3)
                         continue
                else: 
                     logger.critical(f"CRITICAL LOGIC FLAW: Unhandled final_state '{final_state}' for range {date_range_display_str}. Reloading.")
                     if attempt_no < max_attempts_per_chunk: driver.get(WEBSITE_URL); time.sleep(3)
                     continue

            except Exception as e_inner:
                logger.error(f"Error in attempt {attempt_no} (range {date_range_display_str}): {e_inner}", exc_info=True)
                ts = time.strftime("%Y%m%d_%H%M%S")
                try: driver.save_screenshot(f"ERROR_Attempt_{form_from_date_str.replace('-','_')}_to_{form_to_date_str.replace('-','_')}_att{attempt_no}_{ts}.png")
                except: pass
                if attempt_no < max_attempts_per_chunk:
                     logger.info("Reloading after error in chunk processing.")
                     try: driver.get(WEBSITE_URL); time.sleep(4)
                     except Exception as e_reload: logger.error(f"Reload failed: {e_reload}"); break 
                continue 

        if data_processed_this_range: 
            logger.info(f"Successfully processed or confirmed no data for range: {date_range_display_str}.")
            for date_to_mark_yyyymmdd in dates_in_current_chunk_yyyymmdd:
                mark_date_as_processed(date_to_mark_yyyymmdd, processed_dates_coll)
        else:
            logger.error(f"Failed to process range {date_range_display_str} after {max_attempts_per_chunk} attempts. NOT marking dates as processed.")

        wait_time = 4 
        logger.info(f"Waiting {wait_time}s before next date range...")
        time.sleep(wait_time)

    logger.info(f"--- Chunked Scraping loop finished ({num_total_days_to_scrape_backward} days in {DATA_CHUNK_SIZE}-day chunks) ---")
    if driver:
        try: driver.quit(); logger.info("WebDriver closed.")
        except Exception as e: logger.error(f"Error quitting WebDriver: {e}")


# --- Main execution block ---
if __name__ == "__main__":
    try:
        valid_keys = [k for k in GEMINI_API_KEYS if k and not k.startswith("YOUR_GEMINI_API_KEY") and len(k) > 15 and "AIzaS" in k]
        if not valid_keys:
             logger.critical("Valid GEMINI API KEYS NOT CONFIGURED. Exiting.")
             print("CRITICAL: Valid GEMINI API KEYS NOT CONFIGURED. Exiting.")
        else:
             logger.info("Starting script execution (Chunked Mode)...")
             scrape_hc_judgements_chunked()
    except (ValueError, pymongo.errors.ConnectionFailure) as e_setup:
         logger.critical(f"Setup error: {e_setup}. Scraper cannot run.")
         print(f"CRITICAL SETUP ERROR: {e_setup}. Check logs.")
    except KeyboardInterrupt:
        logger.warning("Script interrupted by user.")
        print("\nScript interrupted by user.")
    except Exception as e_main:
        logger.critical(f"Unhandled error: {e_main}", exc_info=True)
        print(f"CRITICAL ERROR: {e_main}. Check logs.")
    finally:
        logging.shutdown()
        logger.info("Chunked scraper script execution ended.")
        print(f"\nChunked scraper script execution ended. Check '{log_file}' for details.")
