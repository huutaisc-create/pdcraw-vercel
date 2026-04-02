import mysql.connector
from mysql.connector import Error
import json
import time
import sys
import os
from io import TextIOWrapper

# Force UTF-8 for stdout (important for subprocess communication)
sys.stdout = TextIOWrapper(sys.stdout.buffer, encoding='utf-8')

import undetected_chromedriver as uc
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
from webdriver_manager.chrome import ChromeDriverManager

# --- DATABASE CONFIG (MARIADB) ---
DB_CONFIG = {
    'user': 'root',
    'password': '123456',
    'host': '127.0.0.1',
    'database': 'pdcraw',
    'charset': 'utf8mb4',
    'collation': 'utf8mb4_unicode_ci',
    'autocommit': True
}

def get_db_connection():
    try:
        return mysql.connector.connect(**DB_CONFIG)
    except Error as e:
        log(f"MariaDB Connection Error: {e}")
        return None
RESULTS_FILE = 'update_results.json'

def log(msg):
    # Log to a separate file to avoid corrupting stdout JSON
    with open('debug_check_update.log', 'a', encoding='utf-8') as f:
        f.write(f"{time.strftime('%Y-%m-%d %H:%M:%S')} - {msg}\n")

def setup_driver():
    log("Setting up ChromeDriver...")
    options = Options()
    options.add_argument("--headless=new")
    options.add_argument("--disable-gpu")
    options.add_argument("--no-sandbox")
    options.add_argument("--window-size=1920,1080")
    options.add_argument("--log-level=3")
    options.add_argument("--blink-settings=imagesEnabled=false") # Disable images for speed
    
    try:
        driver = uc.Chrome(options=options, driver_executable_path=ChromeDriverManager().install())
        log("Driver initialized successfully.")
        return driver
    except Exception as e:
        log(f"Driver setup failed: {e}")
        # We assume if driver fails, we can't do anything.
        sys.exit(1)

def get_ongoing_stories():
    log("Fetching ongoing stories from MariaDB...")
    try:
        conn = get_db_connection()
        if not conn: return []
        cursor = conn.cursor()
        cursor.execute("SELECT id, slug, url, chapters, source FROM stories WHERE book_status != 'Full' AND url IS NOT NULL AND url != ''")
        rows = cursor.fetchall()
        conn.close()
        log(f"DB Query result: {len(rows)} rows found.")
        
        stories = []
        for r in rows:
            stories.append({
                "id": r[0],
                "slug": r[1],
                "url": r[2],
                "current_chapters": r[3] or 0,
                "source": r[4] or 'PD'
            })
        return stories
    except Exception as e:
        log(f"DB Read failed: {e}")
        sys.exit(1)

def scrape_story_info(driver, url):
    log(f"Scraping: {url}")
    try:
        driver.get(url)
        time.sleep(2) # Wait for initial load
        
        title = ""
        chapters = 0
        
        # 1. Try fetching Title
        try: 
            title_elem = driver.find_element(By.TAG_NAME, "h1")
            title = title_elem.text.strip()
        except: 
            pass
        
        # 2. Try fetching Chapters
        # Strategy A: Look for "Danh sách chương (1234)" in buttons/tabs
        found_in_tab = False
        try:
            buttons = driver.find_elements(By.TAG_NAME, "button")
            for btn in buttons:
                txt = btn.text
                if "Danh sách chương" in txt:
                    # Format: "Danh sách chương (123)"
                    if '(' in txt and ')' in txt:
                        val = txt.split('(')[1].split(')')[0]
                        chapters = int(val.replace('.', '').replace(',', ''))
                        found_in_tab = True
                    break
        except Exception as e:
            log(f"Error finding tab: {e}")

        # Strategy B: Look for Metadata "Số chương: 1234"
        if not found_in_tab or chapters == 0:
            try:
                # Iterate all paragraphs or spans looking for key phrase
                # Limiting scope to likely containers might be better, but global search is safer for generic structure
                elements = driver.find_elements(By.XPATH, "//p | //li | //div") 
                # Scan top elements only? No, finding all is fine for text check.
                # Optimization: searching via XPath text contains might be faster
                # but "Số chương" might be in a child node.
                
                # Let's try XPath direct match
                for elem in driver.find_elements(By.XPATH, "//*[contains(text(), 'Số chương')]"):
                    txt = elem.text
                    if "Số chương" in txt:
                        # "Số chương: 1,234"
                        parts = txt.split(':')
                        if len(parts) > 1:
                            val = parts[-1].strip().replace('.', '').replace(',', '')
                            if val.isdigit():
                                chapters = int(val)
                                break
            except Exception as e:
                log(f"Error finding metadata: {e}")

        return {"chapters": chapters, "title": title}
        
    except Exception as e:
        log(f"Scrape failed for {url}: {e}")
        return {"chapters": 0, "title": ""}

def main():
    log("=== START UPDATE CHECK ===")
    
    # Clean old results
    if os.path.exists(RESULTS_FILE):
        try: os.remove(RESULTS_FILE)
        except: pass
        
    stories = get_ongoing_stories()
    
    final_results = []
    
    if not stories:
        log("No ongoing stories to check.")
    else:
        driver = setup_driver()
        try:
            for idx, story in enumerate(stories):
                log(f"Processing {idx+1}/{len(stories)}: {story['slug']}")
                
                info = scrape_story_info(driver, story['url'])
                new_chap = info['chapters']
                
                log(f"  -> Old: {story['current_chapters']}, New: {new_chap}")
                
                if new_chap > story['current_chapters']:
                    final_results.append({
                        "id": story['id'],
                        "slug": story['slug'],
                        "title": info['title'] or story['slug'],
                        "old_chapters": story['current_chapters'],
                        "new_chapters": new_chap,
                        "diff": new_chap - story['current_chapters'],
                        "source": story['source'],
                        "url": story['url']
                    })
                    
        except Exception as e:
            log(f"Critical Loop Error: {e}")
        finally:
            driver.quit()
            
    # Save Results
    # Output only the list [ {...}, ... ] as ManageHandler action='check_update_status' expects a list starting with '['
    try:
        with open(RESULTS_FILE, 'w', encoding='utf-8') as f:
            json.dump(final_results, f, ensure_ascii=False)
        log(f"Saved {len(final_results)} updates to {RESULTS_FILE}")
    except Exception as e:
        log(f"Error saving results: {e}")
        
    log("=== END UPDATE CHECK ===")

if __name__ == "__main__":
    main()
