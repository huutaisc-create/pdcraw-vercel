try:
    import os
    import json
    import time
    import mysql.connector
    from mysql.connector import Error
    import re
    import sys
    import io
    import random
    import signal
    import shutil
    import tempfile
    import socket
    import argparse
    from selenium import webdriver
    from selenium.webdriver.chrome.service import Service
    from selenium.webdriver.chrome.options import Options
    from selenium.webdriver.support.ui import WebDriverWait
    from selenium.webdriver.support import expected_conditions as EC
    from selenium.webdriver.common.by import By
    from selenium.webdriver.common.action_chains import ActionChains
    from selenium.webdriver.common.keys import Keys
    from webdriver_manager.chrome import ChromeDriverManager
    from selenium.common.exceptions import TimeoutException, NoSuchElementException, WebDriverException
    from bs4 import BeautifulSoup
except ImportError as e:
    print(f"\n[!] LỖI THIẾU THƯ VIỆN: {e}")
    print("[*] Vui lòng chạy lệnh sau trên máy này để cài đặt:")
    print("    python -m pip install mysql-connector-python selenium undetected-chromedriver webdriver-manager beautifulsoup4")
    input("\nNhấn Enter để thoát...")
    sys.exit(1)

# Force UTF-8 
# Force UTF-8 with line buffering
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', line_buffering=True)

# --- CONFIG ---

# --- ABSOLUTE PATH CONFIG (DISTRIBUTED SUPPORT) ---
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
IMPORT_DIR = os.path.join(BASE_DIR, 'data_import')

# --- DATABASE CONFIG (MARIADB) ---
DB_CONFIG = {
    'user': 'root',
    'password': '123456',
    'host': '127.0.0.1', # Thay đổi thành IP Máy Chủ nếu chạy từ máy con
    'database': 'pdcraw',
    'charset': 'utf8mb4',
    'collation': 'utf8mb4_unicode_ci',
    'autocommit': True
}

# --- UTILS ---
def find_free_port(max_attempts=20):
    for _ in range(max_attempts):
        try:
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
                s.bind(('localhost', 0))
                port = s.getsockname()[1]
                return port
        except: time.sleep(0.2)
    return 9515 # Default

# --- STARTUP LOCK ---
# Use absolute path to ensure all processes see the same lock file regardless of CWD
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
LOCK_FILE = os.path.join(BASE_DIR, 'startup.lock')

# --- ADAPTIVE NAVIGATION (MATRIX LEARNING) ---
MATRIX_CLICKS = 3    # Current learned optimal clicks (3 or 4)
MATRIX_DELAY = 0.5   # Current learned optimal delay (0.2s to 2.0s)
MATRIX_HISTORY = []  # Track recent successes/failures to adjust

def acquire_startup_lock(timeout_minutes=3):
    # Random sleep to reduce initial stampede
    time.sleep(random.uniform(0.1, 2.0))
    
    print("[*] Checking startup lock...")
    while True:
        try:
            # Try to atomically create the lock file
            # 'x' mode fails if file exists -> Atomic check-and-create
            with open(LOCK_FILE, 'x') as f:
                f.write(str(time.time()))
            
            print("  [+] Acquired startup lock.")
            return True
            
        except FileExistsError:
            # Lock exists, check if stale
            try:
                with open(LOCK_FILE, 'r') as f:
                    content = f.read().strip()
                    timestamp = float(content) if content else 0
                
                # Check if stale (older than timeout)
                if time.time() - timestamp > (timeout_minutes * 60):
                    print("  [!] Found stale lock. Breaking it.")
                    try:
                        os.remove(LOCK_FILE) # Remove and retry loop
                    except: pass
                    continue
                
                # Valid lock, wait
                print("  [.] Waiting for another thread to stabilize (startup.lock)...")
                if os.path.exists('stop.signal'): return False
                time.sleep(3)
                
            except Exception as e:
                # File might be deleted by owner mid-read
                time.sleep(1)
                
        except Exception as e:
            print(f"  [!] Lock Error: {e}")
            time.sleep(1)

def release_startup_lock():
    try:
        if os.path.exists(LOCK_FILE):
            os.remove(LOCK_FILE)
            print("  [+] Released startup lock.")
    except: pass

def get_db_connection():
    config = DB_CONFIG.copy()
    config_file = os.path.join(BASE_DIR, 'db_config.json')
    if os.path.exists(config_file):
        try:
            import json
            with open(config_file, 'r') as f:
                external_config = json.load(f)
                if 'host' in external_config:
                    config['host'] = external_config['host']
        except Exception as e:
            print(f"[!] Error reading db_config.json: {e}")

    try:
        conn = mysql.connector.connect(**config)
        return conn
    except Error as e:
        print(f"[!] MariaDB Connection Error: {e}")
        return None

def claim_next_story(acc_idx=None, admin_name=None):
    conn = get_db_connection()
    cursor = conn.cursor()
    
    # Priority Claim Strategy (Strict V2 + Account Sticky):
    # 0. 'repairing' (Highest Priority)
    # 1. 'selected' + last_account_idx == ME (Resume my own job)
    # 2. 'selected' + downloaded > 0 (Resume any job)
    # 3. 'selected' (New tasks)
    # 4. 'crawling' (Stale recovery)
    
    # We pass acc_idx to logic. If None, ignore.
    my_id = -1 if acc_idx is None else acc_idx
    
    # ADMIN FILTER LOGIC
    # If admin provided, only select stories reserved by admin OR unassigned.
    # If no admin provided (legacy), logic is flexible.
    
    admin_filter = "AND (admin_control IS NULL OR admin_control = '')"
    filter_args = []
    
    if admin_name:
        admin_filter = "AND (admin_control = %s OR admin_control IS NULL OR admin_control = '')"
        filter_args = [admin_name]
    
    query = f"""
        SELECT id, slug, title, url, downloaded_chapters, chapters, crawl_status 
        FROM stories 
        WHERE (
            -- 1. Repair mode (Reserved or unassigned)
            (crawl_status = 'repairing')
            OR 
            -- 2. Selected mode: Pick if assigned to ME or if BRAND NEW (NULL)
            (crawl_status = 'selected' AND (last_account_idx = %s OR last_account_idx IS NULL OR last_account_idx = -1))
            OR
            -- 3. Crawling mode: Resume if it was MINE (no cooldown) OR someone else's stale task (>5m)
            (crawl_status = 'crawling' AND (last_account_idx = %s OR last_updated < NOW() - INTERVAL 5 MINUTE))
        )
        {admin_filter}
        ORDER BY 
            CASE 
                WHEN crawl_status = 'repairing' THEN 0
                WHEN crawl_status = 'crawling' AND last_account_idx = %s THEN 1 -- Resume my own stale task first
                WHEN crawl_status = 'selected' AND last_account_idx = %s THEN 2 
                WHEN crawl_status = 'selected' AND (last_account_idx IS NULL OR last_account_idx = -1) THEN 3
                ELSE 4 
            END ASC,
            last_updated ASC
        LIMIT 1
    """
    
    full_args = [my_id, my_id] + filter_args + [my_id, my_id]
    cursor.execute(query, full_args)
    story = cursor.fetchone()
    
    if story:
        cursor.execute("UPDATE stories SET crawl_status = 'crawling', last_account_idx = %s, last_updated = CURRENT_TIMESTAMP WHERE id = %s", (my_id, story[0],))
    conn.close()
    return story

def update_story_data(story_id, downloaded_count=None, status=None, error=None, total_files=None):
    conn = get_db_connection()
    if not conn: return
    cursor = conn.cursor()
    if downloaded_count is not None:
        cursor.execute("UPDATE stories SET downloaded_chapters = %s, last_updated = CURRENT_TIMESTAMP WHERE id = %s", (downloaded_count, story_id))
    if status:
        cursor.execute("UPDATE stories SET crawl_status = %s, last_updated = CURRENT_TIMESTAMP WHERE id = %s", (status, story_id))
    if error:
        # Mark as error and CLEAR selection to drop from queue
        cursor.execute("UPDATE stories SET crawl_status = 'error', admin_control = NULL, last_account_idx = -1, last_updated = CURRENT_TIMESTAMP WHERE id = %s", (story_id,))
    if total_files is not None:
         cursor.execute("UPDATE stories SET actual_chapters = %s, last_updated = CURRENT_TIMESTAMP WHERE id = %s", (total_files, story_id))
    conn.close()

def sanitize_filename(filename):
    invalid_chars = r'[<>:"/\\|?*\n\r\t]'
    sanitized = re.sub(invalid_chars, '_', filename)
    sanitized = re.sub(r'\s+', '_', sanitized.strip())
    # Limit length just in case
    if len(sanitized) > 100:
        sanitized = sanitized[:100]
    return sanitized

# --- BROWSER LOGIC ---

def setup_driver(acc_idx=0):
    # Use persistent profile based on Thread Index (1-based naming convention)
    # accounts.txt line 0 -> chrome 1
    # accounts.txt line 1 -> chrome 2
    profile_num = acc_idx
    cwd = os.path.dirname(os.path.abspath(__file__))
    profile_dir = os.path.join(cwd, "profiles", f"chrome {profile_num}", "User Data")
    
    if not os.path.exists(profile_dir):
        os.makedirs(profile_dir, exist_ok=True)
        print(f"[*] Creating new persistent profile: chrome {profile_num}")
    else:
        print(f"[*] Using existing profile: chrome {profile_num}")

    port = find_free_port()
    
    options = Options()
    options.add_argument('user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/128.0.0.0 Safari/537.36')
    options.add_argument(f"--user-data-dir={profile_dir}")
    options.add_argument('--disable-blink-features=AutomationControlled')
    options.add_experimental_option('excludeSwitches', ['enable-automation'])
    options.add_experimental_option('useAutomationExtension', False)
    options.add_argument('--no-sandbox')
    options.add_argument('--disable-dev-shm-usage')
    options.add_argument('--start-maximized')
    options.add_argument('--disable-notifications')
    
    service = Service(ChromeDriverManager().install(), port=port)
    driver = webdriver.Chrome(service=service, options=options)
    driver.execute_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")
    
    return driver, profile_dir

def handle_popups(driver):
    try:
        # Overlay click
        try:
             ActionChains(driver).move_by_offset(50, 50).click().perform()
        except: pass

        # Reading Mode Popup
        try:
             scroll_mode_btn = WebDriverWait(driver, 3).until(
                  EC.element_to_be_clickable((By.XPATH, "//div[contains(text(), 'Cuộn xuống để đọc')] | //p[contains(text(), 'Cuộn xuống để đọc')]"))
             )
             scroll_mode_btn.click()
             time.sleep(1)
        except: pass
    except: pass

def get_footer_index(driver):
    """
    Extracts (Current, Total) from footer.
    Supports: "Chương 1390/2925", "1390 / 2925", "Chap 10/100"
    Retries for up to 20 seconds.
    """
    max_retries = 10
    for attempt in range(max_retries):
        try:
            candidates = []
            
            # Strategy A: Specific Footer Selector (Most Reliable)
            try:
                footer_el = driver.find_element(By.CSS_SELECTOR, "div.fixed.bottom-0")
                candidates.append(footer_el)
            except: pass
            
            # Strategy B: Generic Slash Search (Fallback)
            # Only use if A fails or yields no valid text
            if not candidates:
                 elements = driver.find_elements(By.XPATH, "//*[contains(text(), '/')]")
                 candidates.extend(elements)

            if not candidates:
                time.sleep(2)
                continue

            found_any_text = False
            for el in candidates:
                # Use get_attribute("textContent") to get text even if hidden/not rendered fully
                txt = el.get_attribute("textContent").strip()
                if not txt: continue
                
                # print(f"  [DEBUG] Candidate: '{txt}'") 
                found_any_text = True
                
                # 1. Strict Match
                match = re.search(r'Chương\s+(\d+)\s*/\s*(\d+)', txt, re.IGNORECASE)
                if match:
                    # print(f"  [DEBUG] Found strict: {txt}")
                    return int(match.group(1)), int(match.group(2))
                    
                # 2. Relaxed Match
                match_lax = re.search(r'(?:^|\s)(\d+)\s*/\s*(\d+)(?:$|\s)', txt)
                if match_lax:
                    c, t = int(match_lax.group(1)), int(match_lax.group(2))
                    if t > 0: 
                        # print(f"  [DEBUG] Found relaxed: {txt}")
                        return c, t
            
            # If we are here, we found elements but no match.
            if attempt < max_retries - 1:
                if not found_any_text:
                     print(f"  [.] Footer scan attempt {attempt+1}: No text content found. Waiting...")
                else:
                     print(f"  [.] Footer scan attempt {attempt+1}: Text found but no index match. Waiting...")
                time.sleep(2)
            else:
                 # Last attempt failed. Dump debug info of non-empty candidates?
                 pass

        except Exception as e:
            print(f"  [!] Footer Retry Error: {e}")
            time.sleep(1)

    # Final Failure
    try:
        with open("debug_footer_fail.html", "w", encoding="utf-8") as f:
            f.write(driver.page_source)
        print("  [!] Footer extraction failed after retries. Saved debug_footer_fail.html")
    except: pass
    
    return None, None


def scan_menu_and_map(driver, slug):
    """
    Scans the Table of Contents (Menu) and saves mapping to json.
    """
    import json
    map_dir = os.path.join(IMPORT_DIR, slug)
    if not os.path.exists(map_dir): os.makedirs(map_dir)
    map_file = os.path.join(map_dir, "menu_map_v1.json")
    
    # If map exists and is not empty, use it (or maybe force update if needed?)
    # For V1, let's assume if it exists, it's good. But user might want refresh.
    # Let's overwrite for now to ensure we have latest.
    
    print("[*] Mapping Menu...")
    try:
        # Open Menu
        try:
             # Try direct click on menu button
             menu_btn = WebDriverWait(driver, 10).until(
                 EC.element_to_be_clickable((By.CSS_SELECTOR, "button[aria-label='menu']"))
             )
             menu_btn.click()
             time.sleep(2)
        except:
             print("  [!] Failed to open menu safely.")
             return None

        # Container
        try:
            container = driver.find_element(By.CSS_SELECTOR, ".dark-scrollbar")
        except:
            print("  [!] Scroll container not found.")
            return None

        # Scroll deeply (Slow Scroll Strategy)
        print("  [.] Resetting scroll to top...")
        driver.execute_script("arguments[0].scrollTop = 0", container)
        time.sleep(1)
        
        print("  [.] Scrolling menu slowly to load all chapters...")
        last_height = 0
        current_scroll = 0
        total_height = driver.execute_script("return arguments[0].scrollHeight", container)
        
        # Monitor Loop
        waited_stagnant = 0
        collected_titles = [] 
        
        while True:
            # 1. EXTRACT VISIBLE BUTTONS (Continuous Extraction)
            buttons = container.find_elements(By.TAG_NAME, "button")
            visible_titles = []
            for btn in buttons:
                txt = btn.text.strip()
                if txt: visible_titles.append(txt)
                
            # SEQUENTIAL LOGIC (Overlap Merging):
            if not collected_titles:
                collected_titles.extend(visible_titles)
            elif visible_titles:
                # Find maximum overlap between end of collected_titles and start of visible_titles
                # This ensures duplicate chapters are preserved while avoiding double-counting due to scroll overlap
                max_overlap = 0
                max_possible = min(len(collected_titles), len(visible_titles))
                
                # Check overlapping sequences from largest possible to 1
                for overlap_len in range(max_possible, 0, -1):
                    # Does the end of collected match the start of visible?
                    if collected_titles[-overlap_len:] == visible_titles[:overlap_len]:
                        max_overlap = overlap_len
                        break
                
                # Only append the new items that haven't been seen in the overlap
                collected_titles.extend(visible_titles[max_overlap:])
            
            # 2. Check if we reached bottom
            if current_scroll >= total_height:
                 # Check if height expanded (Infinite Scroll)
                 new_total_height = driver.execute_script("return arguments[0].scrollHeight", container)
                 if new_total_height > total_height:
                     total_height = new_total_height
                     print(f"  [i] Menu expanded: {total_height}px")
                     waited_stagnant = 0
                 else:
                     waited_stagnant += 1
                     if waited_stagnant >= 3: 
                         print("  [+] Reached bottom (Stagnant 3x).")
                         break
                     time.sleep(1)
                     continue

            # Scroll Step (Lowered to 350px for safer overlap detection)
            step = 350 
            driver.execute_script(f"arguments[0].scrollBy(0, {step});", container)
            current_scroll += step
            
            # Log
            print(f"  [.] Scrolling: {current_scroll}/{total_height} | Found: {len(collected_titles)}", end='\r')
            time.sleep(0.5) 

        # Final Summary: Build Map Sequentially
        # Item 0 -> Index "1"
        # Item 1 -> Index "2"
        menu_map = {}
        for idx, title in enumerate(collected_titles):
            menu_map[str(idx + 1)] = title
            
        print(f"\n  [+] Menu mapped! Found {len(menu_map)} total chapters (Sequential).")
        
        debug_extracted = []
        sorted_keys = sorted([int(k) for k in menu_map.keys()])
        for k in sorted_keys[:3]: debug_extracted.append(f"{k}: {menu_map[str(k)][:20]}")
        for k in sorted_keys[-3:]: debug_extracted.append(f"{k}: {menu_map[str(k)][:20]}")
        
        if debug_extracted:
            print(f"  [DEBUG] Sample: {debug_extracted[:3]} ... {debug_extracted[-3:]}")
        else:
             print("  [!] WARNING: No items mapped!")

        # Save
        with open(map_file, "w", encoding="utf-8") as f:
            json.dump(menu_map, f, ensure_ascii=False, indent=2)
            
        print(f"  [+] Menu mapped! Found {len(menu_map)} chapters.")
        
        # Close menu (click outside)
        ActionChains(driver).move_by_offset(200, 200).click().perform()
        time.sleep(1)
        
        return menu_map

    except Exception as e:
        print(f"  [!] Menu Map Error: {e}")
        return None

def verify_position(driver, target_index, slug):
    """
    Anchor Logic:
    1. Calculate Anchor = target_index - 1
    2. Check if Anchor is on disk.
    3. If yes, navigate to Anchor in DOM and verify content matches disk.
    """
    anchor_idx = target_index - 1
    if anchor_idx < 1: 
        return True # Can't verify before chap 1
        
    print(f"  [?] Verifying Position (Anchor: {anchor_idx})...")
    
    # 1. Load Anchor Content from Disk
    import glob
    story_dir = os.path.join(IMPORT_DIR, slug)
    pattern = os.path.join(story_dir, f"*_{anchor_idx:04d}.txt")
    files = glob.glob(pattern)
    
    if not files:
        print(f"  [!] Anchor file {anchor_idx} not found on disk. Cannot verify.")
        # Decide: Assume OK or Fail? 
        # If we are continuing a session, it should exist.
        # If we just started, maybe we need to crawl?
        # Let's be strict: If file missing, maybe we are at wrong place or need to redownload.
        # But for now, allow pass to avoid getting stuck if user deleted file.
        return True 
        
    disk_content = ""
    try:
        with open(files[0], "r", encoding="utf-8") as f:
            full = f.read()
            parts = full.split('\n\n', 1)
            if len(parts) > 1:
                disk_content = parts[1].strip()[:100] # First 100 chars
    except: pass
    
    if not disk_content:
        return True # File empty?
        
    # 2. Check DOM
    # We expect to be AT the anchor chapter (since we scrape N, we stand at N-1?)
    # Wait, the logic is: Stand at N-1, Verify N-1, Then Arrow Right to N.
    # So we need to find N-1 in DOM.
    
    # Check "current" chapter in DOM?
    # Logic: Look for the DIV that contains the anchor title or content?
    # Better: Just grab all visible content blocks and see if ANY match disk_content.
    
    try:
         soup = BeautifulSoup(driver.page_source, 'html.parser')
         content_divs = soup.find_all('div', class_=lambda x: x and 'space-y-[30px]' in x)
         
         for div in content_divs:
             dom_text = div.get_text().strip()[:100]
             
             # Similarity check
             # Simple string equality of first 50 chars usually enough
             if disk_content[:50] == dom_text[:50]:
                 print("  [+] Anchor Verified: DOM matches Disk.")
                 return True
                 
         print("  [!] Anchor Mismatch! DOM does not contain Anchor content.")
         return False
         
    except Exception as e:
        print(f"  [!] Verify Error: {e}")
        return False

def crawl_chapter(driver, target_index, menu_map, story_dir):
    """
    Crawl logic using Arrow Keys + Dictionary Check.
    Returns True if success, False if failed (need recovery).
    """
    print(f"  [>] Hunting Chapter {target_index}...")
    
    target_title_full = menu_map.get(str(target_index))
    if not target_title_full:
        print(f"  [!] Chapter {target_index} not found in Menu Map.")
        return False # Should we refresh map?
        
    # Simplify title for matching (Menu might say "Chương 1: ABC", DOM might say just "Chương 1" or "ABC")
    # Actually DOM usually has "Chương X" in the bold header.
    
    for attempt in range(50): # 50 Retries as requested
        # 1. Check if ANY visible block matches target
        soup = BeautifulSoup(driver.page_source, 'html.parser')
        
        # Get pairs of Title + Content
        titles = soup.find_all('div', class_='text-center font-bold')
        contents = soup.find_all('div', class_=lambda x: x and 'space-y-[30px]' in x)
        
        found_content = None
        found_title_text = ""
        
        # Logic: Find title matching expected text from Menu Map
        target_title_clean = re.sub(r'^\d+\s*[-:]?\s*', '', target_title_full).strip().lower()
        
        # Resolve all indices first on the page
        titles_texts = [t.get_text().strip() for t in titles]
        resolved_indices = resolve_page_indices(titles_texts, menu_map)
        
        for i in range(len(titles)):
            idx = resolved_indices[i] if i < len(resolved_indices) else -1
            
            if idx == target_index:
                # Found it!
                if i < len(contents):
                   found_content = contents[i].get_text("\n").strip()
                   found_title_text = titles_texts[i]
                   break
        
        if found_content:
             is_valid_content = len(found_content) > 50
             if not is_valid_content:
                 whitelist_pattern = r"(thieu\s*chuong|thiếu\s*chương|nhay\s*chuong|nhảy\s*chương|mat\s*chuong|mất\s*chương|cập\s*nhật|lỗi\s*chương|trùng\s)"
                 if re.search(whitelist_pattern, found_content, re.IGNORECASE):
                     is_valid_content = True

             if is_valid_content:
                 # Success
                 print(f"  [+] Found Chapter {target_index}: '{found_title_text}' (Attempt {attempt+1})")
                 
                 # Save
                 safe_title = re.sub(r'[\\/*?:"<>|]', "", found_title_text).strip()
                 if not safe_title: safe_title = f"Chapter_{target_index}"
                 fname = f"{safe_title}_{target_index:04d}.txt"
                 
                 with open(os.path.join(story_dir, fname), "w", encoding="utf-8") as f:
                     f.write(f"{found_title_text}\nIndex:{target_index}\n\n{found_content}")
                     
                 return True
             
        # Not found -> Arrow Right
        ActionChains(driver).send_keys(Keys.ARROW_RIGHT).perform()
        time.sleep(0.3)
        
    print(f"  [!] Failed to find Chapter {target_index} after 50 retries.")
    return False


def smart_navigate_absolute(driver, target_idx, menu_map=None, force_reload=False):
    """
    BEST EFFORT JUMP:
    Opens menu, scrolls to target_idx, clicks the button, and returns.
    No strict verification or arrow correction (handled by Hunter).
    """
    print(f"[NAV] Seeking Absolute Chapter: {target_idx} (Force: {force_reload})")
    
    for attempt in range(2):
        if attempt > 0:
            print("  [!] First nav attempt failed. Refreshing page and retrying...")
            try:
                driver.refresh()
                time.sleep(5)
                # Ensure menu is interactable again
                ActionChains(driver).move_by_offset(10, 10).click().perform() 
            except: pass

        # 1. Simple fast check
        if not force_reload and attempt == 0:
            current = get_current_page_idx(driver, menu_map)
            if current == target_idx:
                print(f"  [+] Already at {target_idx}.")
                return True
        
        # 2. Main Navigation Block
        try:
            # Open Menu
            try:
                 try: ActionChains(driver).move_by_offset(10, 10).click().perform()
                 except: pass
                 
                 menu_btn = WebDriverWait(driver, 5).until(
                     EC.element_to_be_clickable((By.CSS_SELECTOR, "button[aria-label='menu']"))
                 )
                 menu_btn.click()
                 time.sleep(3)
            except:
                 print("  [!] Menu open failed.")
                 if attempt == 0: continue 
                 return False

            # Find Scroll Container
            try:
                container = driver.find_element(By.CSS_SELECTOR, ".dark-scrollbar")
            except:
                print("  [!] Scroll container not found.")
                if attempt == 0: continue
                return False

            # MEASURE HEIGHT (Diagnostic/Learning)
            btn_h = driver.execute_script("""
                const btns = arguments[0].querySelectorAll('button');
                if (btns.length >= 2) {
                    return btns[1].offsetTop - btns[0].offsetTop;
                }
                return 35.0;
            """, container)
            
            # Calculate Scroll
            scroll_top = max(0, (target_idx - 1) * btn_h)
            print(f"  [DEBUG] Scrolling to {scroll_top}px (h={btn_h})...")
            driver.execute_script("arguments[0].scrollTop = arguments[1]", container, scroll_top)
            time.sleep(1.0) 

            buttons = container.find_elements(By.TAG_NAME, "button")
            if buttons:
                target_btn = None
                
                # 1. Prepare Target Info
                expected_title = ""
                expected_num = str(target_idx)
                if menu_map and str(target_idx) in menu_map:
                    expected_title = menu_map[str(target_idx)].strip()
                    m = re.search(r'(?:Chương|Chap|Chapter)\s*(\d+)', expected_title, re.I)
                    if m: expected_num = m.group(1)
                
                # 2. PRIORITY: Match by FULL Title (Precision Jump)
                if expected_title:
                    for btn in buttons:
                        txt = btn.text.strip()
                        if expected_title.lower() in txt.lower():
                            target_btn = btn
                            print(f"  [>] Precision Match found by Title: '{txt}'")
                            break
                            
                # 3. SECONDARY: Match by Chapter Number
                if not target_btn:
                    for btn in buttons:
                        if re.search(r'\b' + expected_num + r'\b', btn.text):
                            target_btn = btn
                            print(f"  [>] Match found by Number: '{expected_num}'")
                            break
                
                # 4. Fallback: Middle button
                if not target_btn:
                     target_btn = buttons[len(buttons) // 2]
                     print(f"  [>] No Title/Number match. Falling back to middle button.")
                
                target_btn.click()
                print(f"  [DEBUG] Clicked button (Target Index {target_idx}).")
                time.sleep(5) 
                return True
                
        except Exception as e:
            print(f"  [!] Nav Error: {e}")
            
    return False

def login_procedure(driver, account_index=0):
    print(f"[*] Starting Login Procedure (Account Index: {account_index})...")
    try:
        if not os.path.exists('accounts.txt'): return False
        with open('accounts.txt', 'r', encoding='utf-8') as f:
            lines = [l.strip() for l in f if '|' in l and not l.startswith('#')]
        if not lines or account_index >= len(lines): return False
        
        email, password = lines[account_index].split('|')
        print(f"[*] Checking Login state for {email}...")
        
        # 1. Start at Login Page to be safe for Re-login
        # Or better: Go to Home/Read URL and check for "Đăng nhập" button
        # But User says: "Click Đăng Nhập will go to Login Page"
        # So simplest is: Go straight to Login Page.
        # If logged in -> Redirects to /user.
        # If not -> Shows form.
        
        driver.get("https://truyenphuongdong.com/login")
        time.sleep(3)
        
        # Dismiss app download popup if present
        try:
            close_btn = WebDriverWait(driver, 5).until(
                EC.element_to_be_clickable((By.CSS_SELECTOR, 'button[aria-label="Đóng"]'))
            )
            close_btn.click()
            time.sleep(1)
            print("  [+] Dismissed app popup.")
        except: pass
        
        # Check if already logged in (Redirected to /user or /tim-truyen)
        if "/user" in driver.current_url:
             print("  [+] Already Logged In (Redirected to /user).")
             return True
             
        # Check if "Đăng nhập" button is present (meaning we are NOT logged in, even if on other page)
        # But we are at /login now.
        
        print(f"[*] Performing login form submission...")
        try:
            user_input = WebDriverWait(driver, 10).until(
                EC.presence_of_element_located((By.CSS_SELECTOR, "input[name='email'], input[name='username']"))
            )
            pass_input = driver.find_element(By.CSS_SELECTOR, "input[name='password']")
            
            # CLEAR INPUTS
            user_input.click()
            user_input.clear()
            user_input.send_keys(Keys.CONTROL + "a"); user_input.send_keys(Keys.DELETE)
            
            pass_input.click()
            pass_input.clear()
            pass_input.send_keys(Keys.CONTROL + "a"); pass_input.send_keys(Keys.DELETE)
            
            time.sleep(1)
            user_input.send_keys(email)
            pass_input.send_keys(password)
            
            driver.find_element(By.CSS_SELECTOR, "button[type='submit']").click()
        except Exception as e:
            # Maybe already logged in and element not found?
            if "/user" in driver.current_url: return True
            print(f"  [!] Form Error: {e}")
            return False
            
        try:
            # Wait for redirect to /user
            WebDriverWait(driver, 30).until(EC.url_contains("/user"))
            print("  [+] Login Success (Redirected to /user)")
            return True
        except:
             print("  [!] Login Failed (No redirect to /user)")
             return False
    except Exception as e:
        print(f"  [!] Login Process Error: {e}")
        return False

def ensure_login_session(driver, account_index=0, target_url=None):
    if "Vui lòng đăng nhập" in driver.page_source:
        print("[!] LOGIN WALL DETECTED!")
        while True:
            if login_procedure(driver, account_index):
                if target_url:
                    print(f"  [>] Login Success. Returning to story: {target_url}")
                    driver.get(target_url)
                else:
                    driver.refresh()
                time.sleep(5)
                return True
            time.sleep(60)
    return True

# --- NEW LOGIC V2 ---

def read_prev_chapter_anchor(story_dir, chapter_idx):
    """
    Reads the LAST 150 chars of the previous chapter file to use as an anchor.
    """
    if chapter_idx <= 1: return None
    
    prev_idx = chapter_idx - 1
    suffix = f"_{prev_idx:04d}.txt"
    
    try:
        if not os.path.exists(story_dir): return None
        found_file = None
        for f in os.listdir(story_dir):
            if f.endswith(suffix):
                found_file = os.path.join(story_dir, f)
                break
        
        if found_file:
            with open(found_file, "r", encoding="utf-8") as f:
                content = f.read().strip()
                if len(content) > 100:
                    return content[-150:].strip() 
                return content
    except Exception as e: 
        print(f"  [!] Anchor Read Error: {e}")
    return None

def is_already_saved(story_dir, target_idx):
    """ Fast check if a chapter index already exists on disk. """
    try:
        suffix = f"_{target_idx:04d}.txt"
        for f in os.listdir(story_dir):
            if f.endswith(suffix): return True
    except: pass
    return False

def scrape_and_save_single(driver, target_idx, menu_map, story_dir):
    """ Scans DOM specifically for one target_idx, scrapes and saves it. """
    try:
        soup = BeautifulSoup(driver.page_source, 'html.parser')
        titles_divs = soup.find_all('div', class_='text-center font-bold')
        contents = soup.find_all('div', class_=lambda x: x and 'space-y-[30px]' in x)
        
        limit = min(len(titles_divs), len(contents))
        if limit == 0: return False
        
        # Resolve indices for all visible titles to handle duplicates
        titles = [titles_divs[i].get_text().strip() for i in range(limit)]
        resolved_indices = resolve_page_indices(titles, menu_map)
        
        for i in range(limit):
            idx = resolved_indices[i] if i < len(resolved_indices) else -1
            
            if idx == target_idx:
                c_text = contents[i].get_text("\n").strip()
                is_valid_content = len(c_text) > 50
                if not is_valid_content:
                    whitelist_pattern = r"(thieu\s*chuong|thiếu\s*chương|nhay\s*chuong|nhảy\s*chương|mat\s*chuong|mất\s*chương)"
                    if re.search(whitelist_pattern, c_text, re.IGNORECASE):
                        is_valid_content = True

                if is_valid_content:
                    save_chapter(story_dir, target_idx, titles[i], c_text)
                    print(f"  [+] Saved Chapter {target_idx}: '{titles[i]}'")
                    return True
    except: pass
    return False

def poll_footer_fast(driver):
    """ Non-blocking fast check for footer index. """
    try:
        # Strategy A: Specific Footer
        footer_el = None
        try: footer_el = driver.find_element(By.CSS_SELECTOR, "div.fixed.bottom-0")
        except: pass
        
        # Strategy B: Slash Search
        candidates = [footer_el] if footer_el else []
        if not candidates:
             candidates = driver.find_elements(By.XPATH, "//*[contains(text(), '/')]")

        for el in candidates:
            txt = el.get_attribute("textContent").strip()
            if not txt: continue
            
            # Strict
            m = re.search(r'Chương\s+(\d+)\s*/\s*(\d+)', txt, re.IGNORECASE)
            if m: return int(m.group(1)), int(m.group(2))
            
            # Lax
            ml = re.search(r'(?:^|\s)(\d+)\s*/\s*(\d+)(?:$|\s)', txt)
            if ml: return int(ml.group(1)), int(ml.group(2))
    except: pass
    return None, None

def crawl_chapter_v2(driver, target_idx, menu_map, story_dir):
    """
    V2 Strategy: Proactive Forward Hunter
    - Attempt 1: Window Save -> 20-Click Hunt from CURRENT position.
    - Attempt 2 (Fallback): If Hunter fails, do a Menu Jump to N+1 -> Window Save -> 20-Click Hunt.
    - Hard Timeout: If total time for this chapter exceeds 120s, return "RESTART".
    """
    if is_already_saved(story_dir, target_idx):
        return True

    start_time = time.time()

    for attempt in range(2):
        if attempt == 0:
            # Attempt 1: Always try to "Hunt" from where we are first
            print(f"  [>] Hunting Chapter {target_idx} from current position...")
        else:
            # Attempt 2: If Hunter fails, then use Menu Jump to Target (N) as fallback
            print(f"  [!] Hunter failed. Attempting Resync Jump directly to Target ({target_idx})...")
            smart_navigate_absolute(driver, target_idx, menu_map=menu_map, force_reload=True)
            cho_load(driver)

        # 2. PROACTIVE WINDOW SAVE (N, N+1, N+2...)
        print(f"  [>] Hunting Chapter {target_idx} (Attempt {attempt+1})...")
        for w_check in range(3):
            if time.time() - start_time > 120: return "RESTART"
            
            visible = get_all_visible_indices(driver, menu_map)
            for vix in visible:
                if not is_already_saved(story_dir, vix):
                    scrape_and_save_single(driver, vix, menu_map, story_dir)
            
            if is_already_saved(story_dir, target_idx): return True
            time.sleep(1)

        # 3. SMART HUNTER LOOP (50 clicks max safety, Stop on Footer Match)
        print(f"  [H] Starting Smart Hunter (Max 50 clicks, Target: {target_idx})...")
        for click in range(1, 51):
            if time.time() - start_time > 120: return "RESTART"

            ActionChains(driver).send_keys(Keys.ARROW_RIGHT).perform()
            time.sleep(1)
            
            # A. Opportunistic Save (Save EVERYTHING visible)
            visible = get_all_visible_indices(driver, menu_map)
            for vix in visible:
                if not is_already_saved(story_dir, vix):
                    scrape_and_save_single(driver, vix, menu_map, story_dir)
            
            if is_already_saved(story_dir, target_idx): return True
            
            # B. Check Footer Position
            curr, total = poll_footer_fast(driver)
            if curr:
                if curr == target_idx:
                    # We are AT the target page index.
                    # Verify content one last time.
                    if is_already_saved(story_dir, target_idx): return True
                    
                    print(f"  [H] Reached Footer Index {curr}, but Content handling failed/pending...")
                    # Give it a moment to load content?
                    time.sleep(2)
                    scrape_and_save_single(driver, target_idx, menu_map, story_dir)
                    if is_already_saved(story_dir, target_idx): return True
                    
                    print(f"  [H] Footer says {curr} but no content found. Forcing Menu Jump...")
                    break # Break loop -> Trigger Fallback Jump

                if curr > target_idx:
                     print(f"  [H] Overshot! Footer says {curr} > Target {target_idx}. Forcing Menu Jump...")
                     break # Break loop -> Trigger Fallback Jump
                
                # If curr < target_idx, continue clicking Arrow Right
                if click % 5 == 0: print(f"  [H] Hunter Tracking: {curr} -> {target_idx}...")

    print(f"  [!] Smart Hunter finished (Target {target_idx} not found). Fallback to Jump.")
    # Function end -> loops back to Attempt 2 (Jump) or Returns RESTART if attempts exhausted.
    # Actually wait, the loop structure is: for attempt in range(2).
    # If attempt 0 breaks, it goes to attempt 1 (Jump). Correct.
    if attempt == 1: return "RESTART" # If jump also failed, restart.

def save_chapter(story_dir, target_idx, title, content):
    safe_title = re.sub(r'[\\/*?:"<>|]', "", title).strip()
    if not safe_title: safe_title = f"Chapter_{target_idx}"
    fname = f"{safe_title}_{target_idx:04d}.txt"
    
    # Check for contiguous duplicate (target_idx - 1)
    if target_idx > 1:
        prev_idx = target_idx - 1
        prev_files = [f for f in os.listdir(story_dir) if f.endswith(f"_{prev_idx:04d}.txt")]
        if prev_files:
            prev_file_path = os.path.join(story_dir, prev_files[0])
            try:
                with open(prev_file_path, "r", encoding="utf-8") as f:
                    prev_lines = f.read().splitlines()
                # 0: title, 1: Index:x, 2: empty, 3+: content
                if len(prev_lines) >= 4:
                    prev_title = prev_lines[0].strip()
                    prev_content = "\n".join(prev_lines[3:]).strip()
                    
                    # If title AND content exactly match, it's a perfect duplicate
                    if prev_title == title.strip() and prev_content == content.strip():
                        print(f"  [i] Duplicate detected: Chapter {target_idx} is identical to {prev_idx}. Removing old file.")
                        os.remove(prev_file_path)
            except Exception as e:
                print(f"  [!] Duplicate check error: {e}")

    with open(os.path.join(story_dir, fname), "w", encoding="utf-8") as f:
        f.write(f"{title}\nIndex:{target_idx}\n\n{content}")

# --- HELPER FUNCTIONS FOR HYBRID NAV ---

def check_title_on_page(driver, target_title_map):
    """
    Scans the current page for a Title Block matching target_title_map.
    Returns (title_found, content_text) if found, else (None, None).
    """
    try:
        soup = BeautifulSoup(driver.page_source, 'html.parser')
        all_titles = soup.find_all('div', class_='text-center font-bold')
        
        for i, t_div in enumerate(all_titles):
            t_text = t_div.get_text().strip()
            
            # Fuzzy match
            clean_map = re.sub(r'\s+', ' ', target_title_map.strip().lower())
            clean_page = re.sub(r'\s+', ' ', t_text.strip().lower())
            
            if clean_map in clean_page or clean_page in clean_map:
                # Get Content
                all_content = soup.find_all('div', class_=lambda x: x and 'space-y-[30px]' in x)
                if i < len(all_content):
                     content_div = all_content[i]
                     raw_text = content_div.get_text("\n").strip()
                     if len(raw_text) > 50:
                         return t_text, raw_text
        return None, None
    except:
        return None, None

def cho_load(driver, timeout_max=30):
    print("  [.] cho_load: Waiting for page to stabilize...")
    # 1. Wait for tab to stop spinning
    try:
        WebDriverWait(driver, timeout_max).until(
            lambda d: d.execute_script('return document.readyState') == 'complete'
        )
    except:
        print("  [!] cho_load: Timeout waiting for document.readyState == 'complete'")
    
    # 2. Add 5s buffer
    time.sleep(5)

def extract_chapter_number(t_text):
    """
    Extracts base chapter number and any appended suffix part, e.g. '(1)' or 'Phần 1'
    Returns string like '131 (1)', '129-2', or '145'.
    """
    tm = re.search(r'(?:Chương|Chap|Chapter)\s*(\d+(?:-\d+)?)', t_text, re.I)
    if not tm: return None
    base = tm.group(1)
    
    # Check for trailing suffixes like (1), [1], Phần 1
    sm = re.search(r'(\(\d+\)|\[\d+\]|Phần\s*\d+)', t_text, re.I)
    if sm:
        return f"{base} {sm.group(1)}"
    return base

def match_title_to_index(t_text, menu_map):
    """ Helper to map a DOM title string to a sequential Index from menu_map. """
    if not menu_map: return -1
    t_clean = re.sub(r'\s+', ' ', t_text.lower()).strip()
    
    # Extract chapter number with suffix
    num_on_page = extract_chapter_number(t_text)

    for idx_str, title_val in menu_map.items():
        v_clean = re.sub(r'\s+', ' ', title_val.lower()).strip()
        # Full match or fuzzy match
        if t_clean == v_clean or (len(t_clean) > 15 and t_clean in v_clean):
            return int(idx_str)
        
        # Fallback: Match by Chapter Number & Suffix
        if num_on_page:
             vm_num = extract_chapter_number(title_val)
             if vm_num == num_on_page:
                 return int(idx_str)
    return -1

def resolve_page_indices(titles, menu_map):
    """
    Given a list of titles visible on the page, find their exact starting index in menu_map
    by matching the sequence of titles. This resolves duplicate chapter titles.
    Returns a list of resolved indices corresponding to the input titles.
    """
    if not menu_map or not titles:
        return []
        
    titles_clean = [re.sub(r'\s+', ' ', t.lower()).strip() for t in titles]
    map_values = [re.sub(r'\s+', ' ', v.lower()).strip() for v in menu_map.values()]
    map_keys = list(menu_map.keys())
    
    # --- HOTFIX: Group identical adjacent titles ---
    # Because hunting presses right arrow multiple times, the lazy load might inject
    # the exact same chapter block 3-4 times. We must group them so sequence matcher 
    # doesn't think 4 identical titles = 4 sequential chapters in the map.
    grouped_titles = []
    group_counts = []
    for t in titles_clean:
        if not grouped_titles or t != grouped_titles[-1]:
            grouped_titles.append(t)
            group_counts.append(1)
        else:
            group_counts[-1] += 1
            
    # Now try to match `grouped_titles` sequence against `map_values`
    best_match_idx = -1
    max_matches = 0
    
    for i in range(len(map_values) - len(grouped_titles) + 1):
        matches = 0
        for j in range(len(grouped_titles)):
            # Check full match or fuzzy match for the sequence element
            if grouped_titles[j] == map_values[i + j] or (
                len(grouped_titles[j]) > 10 and grouped_titles[j] in map_values[i + j]
            ):
                matches += 1
        
        if matches > max_matches:
            max_matches = matches
            best_match_idx = i
            if max_matches == len(grouped_titles):
                break # Perfect sequence match found
                
    resolved_indices = []
    
    # 2. Sequence matched significantly
    if max_matches > 0 and (max_matches >= len(grouped_titles) // 2 or max_matches >= 2):
        # We found a sequence. Now unpack the group_counts to assign the same index to identical adjacent DOM elements.
        for g_idx, count in enumerate(group_counts):
            mapped_index = int(map_keys[best_match_idx + g_idx])
            resolved_indices.extend([mapped_index] * count)
        return resolved_indices
        
    # 3. Fallback: Resolve individually if sequence matching failed
    return [match_title_to_index(t, menu_map) for t in titles]

def get_all_visible_indices(driver, menu_map=None):
    """
    Scans DOM for all visible chapter indices.
    Returns a sorted list of unique indices found on page.
    """
    indices = set()
    try:
        soup = BeautifulSoup(driver.page_source, 'html.parser')
        # 1. Index markers
        markers = re.findall(r'Index:(\d+)', soup.get_text())
        for m in markers: indices.add(int(m))
        
        # 2. Titles matching menu_map sequentially
        titles_divs = soup.find_all('div', class_='text-center font-bold')
        titles = [t.get_text().strip() for t in titles_divs]
        
        resolved_indices = resolve_page_indices(titles, menu_map)
        for idx in resolved_indices:
            if idx != -1: indices.add(idx)
            
    except: pass
    return sorted(list(indices))

def get_current_page_idx(driver, menu_map=None):
    """
    Identifies current chapter index.
    DOM IS THE ULTIMATE TRUTH (Layer 2).
    """
    try:
        soup = BeautifulSoup(driver.page_source, 'html.parser')
        
        # 1. PRIORITY: Check DOM markers (Index:XXX)
        m = re.search(r'Index:(\d+)', soup.get_text())
        if m: return int(m.group(1))
        
        # 2. PRIORITY: Match Title Sequence in DOM against menu_map
        titles_divs = soup.find_all('div', class_='text-center font-bold')
        if titles_divs:
            titles = [t.get_text().strip() for t in titles_divs]
            resolved_indices = resolve_page_indices(titles, menu_map)
            
            for idx in resolved_indices:
                if idx != -1:
                    return idx  # Return the first valid resolved index
                
            # Fallback Regex if no map match found yet
            for t_div in titles_divs:
                num_str = extract_chapter_number(t_div.get_text())
                if not menu_map and num_str: 
                    # If no map exists, return just the base integer before any dash/space
                    return int(re.split(r'[- ]', num_str)[0])

    except: pass
    return -1

def adaptive_matrix_probe(driver, target_idx, menu_map=None):
    """
    Phase 6: Adaptive Matrix Learning
    Uses (MATRIX_CLICKS, MATRIX_DELAY) to find target_idx.
    Adapts on lag, fail, or overshoot.
    """
    global MATRIX_CLICKS, MATRIX_DELAY, MATRIX_HISTORY
    
    start_idx = get_current_page_idx(driver, menu_map)
    print(f"  [Matrix] Target: {target_idx} | Current: {start_idx} | Config: ({MATRIX_CLICKS} clicks, {MATRIX_DELAY}s)")

    if start_idx == target_idx: 
        print(f"  [+] Already at Target {target_idx}.")
        return True

    # --- PROBE FORWARD ---
    for _ in range(MATRIX_CLICKS):
        ActionChains(driver).send_keys(Keys.ARROW_RIGHT).perform()
        time.sleep(MATRIX_DELAY)
    
    # Wait for stabilizing
    time.sleep(1)
    new_idx = get_current_page_idx(driver, menu_map)
    
    print(f"  [Matrix] Result: {new_idx}")

    if new_idx == target_idx:
        # SUCCESS! Maybe optimize/confirm config
        print(f"  [+] Matrix HIT! Reached Chapter {target_idx}")
        MATRIX_HISTORY.append('success')
        return True
    
    if new_idx > target_idx:
        # OVERSHOT -> Backtrack + Adjust
        print(f"  [!] Matrix OVERSHOT to {new_idx}. Backtracking...")
        while get_current_page_idx(driver, menu_map) > target_idx:
            ActionChains(driver).send_keys(Keys.ARROW_LEFT).perform()
            time.sleep(0.5)
        
        # Adjust Matrix
        if MATRIX_CLICKS > 3: MATRIX_CLICKS = 3
        else: MATRIX_DELAY = min(2.0, MATRIX_DELAY + 0.1) 
        MATRIX_HISTORY = [] # Reset learning
        final_idx = get_current_page_idx(driver, menu_map)
        return final_idx == target_idx

    if new_idx < target_idx or new_idx == -1:
        # LAG or STILL BEHIND -> Increase Delay or Clicks
        print(f"  [!] Matrix LAGGED/Behind. Current: {new_idx}, Target: {target_idx}. Increasing Delay...")
        MATRIX_DELAY = min(2.0, MATRIX_DELAY + 0.2)
        if MATRIX_DELAY >= 1.5 and MATRIX_CLICKS < 4: MATRIX_CLICKS = 4
        
        # Retry once with new config
        for _ in range(2): # Extra nudge
             ActionChains(driver).send_keys(Keys.ARROW_RIGHT).perform()
             time.sleep(MATRIX_DELAY)
        
        final_idx = get_current_page_idx(driver, menu_map)
        return final_idx == target_idx

    return False

def ping_pong_hunt(driver, target_title_map):
    # Wrapper for compatibility or specialized hunt
    return adaptive_matrix_probe(driver, -1) # Placeholder
        
    return None, None

    return None, None

def scan_menu_and_map(driver, slug, read_url=None):
    """
    Scans the Table of Contents (Menu) and saves mapping to json.
    Restored from Backup PDcraw-10-02.
    """
    import json
    map_dir = os.path.join(IMPORT_DIR, slug)
    if not os.path.exists(map_dir): os.makedirs(map_dir)
    map_file = os.path.join(map_dir, "menu_map_v1.json")
    
    print(f"[*] Mapping Menu... (Ensuring URL: {read_url})")
    try:
        # Open Menu
        try:
             # Try direct click on menu button
             menu_btn = WebDriverWait(driver, 10).until(
                 EC.element_to_be_clickable((By.CSS_SELECTOR, "button[aria-label='menu']"))
             )
             menu_btn.click()
             time.sleep(2)
        except:
             print("  [!] Failed to open menu safely.")
             return None

        # Container
        try:
            container = driver.find_element(By.CSS_SELECTOR, ".dark-scrollbar")
        except:
            print("  [!] Scroll container not found.")
            return None

        # Scroll deeply (Slow Scroll Strategy)
        print("  [.] Resetting scroll to top...")
        driver.execute_script("arguments[0].scrollTop = 0", container)
        time.sleep(1)
        
        print("  [.] Scrolling menu slowly to load all chapters...")
        last_height = 0
        current_scroll = 0
        total_height = driver.execute_script("return arguments[0].scrollHeight", container)
        
        # Monitor Loop
        waited_stagnant = 0
        collected_titles = [] 
        
        while True:
            # 1. EXTRACT VISIBLE BUTTONS (Continuous Extraction)
            buttons = container.find_elements(By.TAG_NAME, "button")
            visible_titles = []
            for btn in buttons:
                txt = btn.text.strip()
                if txt: visible_titles.append(txt)
                
            # SEQUENTIAL LOGIC (Overlap Merging):
            if not collected_titles:
                collected_titles.extend(visible_titles)
            elif visible_titles:
                # Find maximum overlap between end of collected_titles and start of visible_titles
                # This ensures duplicate chapters are preserved while avoiding double-counting due to scroll overlap
                max_overlap = 0
                max_possible = min(len(collected_titles), len(visible_titles))
                
                # Check overlapping sequences from largest possible to 1
                for overlap_len in range(max_possible, 0, -1):
                    # Does the end of collected match the start of visible?
                    if collected_titles[-overlap_len:] == visible_titles[:overlap_len]:
                        max_overlap = overlap_len
                        break
                
                # Only append the new items that haven't been seen in the overlap
                collected_titles.extend(visible_titles[max_overlap:])
            
            # 2. Check if we reached bottom
            if current_scroll >= total_height:
                 # Check if height expanded (Infinite Scroll)
                 new_total_height = driver.execute_script("return arguments[0].scrollHeight", container)
                 if new_total_height > total_height:
                     total_height = new_total_height
                     print(f"  [i] Menu expanded: {total_height}px")
                     waited_stagnant = 0
                 else:
                     waited_stagnant += 1
                     if waited_stagnant >= 3: 
                         print("  [+] Reached bottom (Stagnant 3x).")
                         break
                     time.sleep(1)
                     continue

            # Scroll Step
            step = 800 
            driver.execute_script(f"arguments[0].scrollBy(0, {step});", container)
            current_scroll += step
            
            # Log
            print(f"  [.] Scrolling: {current_scroll}/{total_height} | Found: {len(collected_titles)}", end='\r')
            time.sleep(0.5) 

        # Final Summary: Build Map Sequentially
        # Item 0 -> Index "1"
        # Item 1 -> Index "2"
        menu_map = {}
        for idx, title in enumerate(collected_titles):
            menu_map[str(idx + 1)] = title
            
        print(f"\n  [+] Menu mapped! Found {len(menu_map)} total chapters (Sequential).")
        
        # Save
        with open(map_file, "w", encoding="utf-8") as f:
            json.dump(menu_map, f, ensure_ascii=False, indent=2)
            
        print(f"  [+] Menu mapped! Found {len(menu_map)} chapters.")
        
        # Close menu (click outside)
        try:
            ActionChains(driver).move_by_offset(200, 200).click().perform()
            time.sleep(1)
        except Exception as cm_err:
             # Ignore move target out of bounds
             print(f"  [!] Note: Menu close ignored ({cm_err}). Map saved ok.")
        
        return menu_map

    except Exception as e:
        print(f"  [!] Menu Map Error: {e}")
        return None

def process_story(driver, story_data, acc_idx=0):
    story_id, slug, title, url, downloaded_idx, total_db, status = story_data
    
    conn = None # Initialize conn to None
    try:
        conn = get_db_connection()

        story_dir = os.path.join(IMPORT_DIR, slug)
        if not os.path.exists(story_dir): os.makedirs(story_dir)
        
        print(f"[*] Processing: {title} ({slug})")
        read_url = url.replace('/sach/', '/read/')
        driver.get(read_url)
        time.sleep(5)
        # Ensure login logic first
        ensure_login_session(driver, acc_idx, read_url)
        handle_popups(driver)
        
        # LOAD MENU MAP (Crucial for Verification)
        menu_map = {}
        map_path = os.path.join(story_dir, "menu_map_v1.json")
        if os.path.exists(map_path):
             import json
             with open(map_path, 'r', encoding='utf-8') as f:
                 menu_map = json.load(f)
                 print(f"  [i] Loaded Menu Map ({len(menu_map)} entries).")
        else:
             print("  [!] Menu Map NOT FOUND! Scanning Menu from scratch...")
             menu_map = scan_menu_and_map(driver, slug, read_url)
             if not menu_map:
                 print("  [!] Failed to create Menu Map. Cannot proceed.")
                 cursor = conn.cursor()
                 cursor.execute("UPDATE stories SET crawl_status='paused', last_updated=CURRENT_TIMESTAMP WHERE id=%s", (story_id,))
                 return False
        
        # Determine Range
        total_idx = len(menu_map)
        start_target = (downloaded_idx or 0) + 1
        
        print(f"  [i] Target Range: {start_target} -> {total_idx}")
        
        if start_target > total_idx:
            print("  [+] All chapters downloaded.")
            return True

        # --- INITIAL POSITIONING (One-time at startup) ---
        # Logic Hunter: Jump directly to Start Target (N) to ensure it is loaded.
        print(f"  [i] Initial Positioning: jumping to Start Target ({start_target})...")
        smart_navigate_absolute(driver, start_target, menu_map=menu_map, force_reload=True)
        cho_load(driver)

        chapters_saved_session = 0
        
        # MAIN LOOP: Refined Navigation (Adaptive V2 Strategy)
        for expected_next in range(start_target, total_idx + 1):
            if os.path.exists('stop.signal'): break
            ensure_login_session(driver, acc_idx, read_url)
            
            # --- PHASE 6: ADAPTIVE CRAWL V2 ---
            # crawl_chapter_v2 handles searching, matching, and saving
            result = crawl_chapter_v2(driver, expected_next, menu_map, story_dir)
            
            if result == True:
                # Update DB and Progress
                chapters_saved_session += 1
                if chapters_saved_session == 1: release_startup_lock()
                
                cursor = conn.cursor()
                cursor.execute("UPDATE stories SET downloaded_chapters = %s, last_updated = CURRENT_TIMESTAMP WHERE id = %s", (expected_next, story_id))
                conn.commit()

                # --- COMPLETION LOGIC ---
                if expected_next == total_idx:
                    print(f"  [***] Story '{title}' COMPLETED!")
                    cursor.execute("UPDATE stories SET crawl_status = 'completed', last_updated = CURRENT_TIMESTAMP WHERE id = %s", (story_id,))
                    conn.commit()
            elif result == "RESTART":
                 print(f"  [!] Chapter {expected_next} signaled RESTART. Exiting story loop...")
                 return "RESTART"
            else:
                 # Unexpected failure
                 print(f"  [!] Unexpected failure on Chapter {expected_next}. Restarting...")
                 return "RESTART"
            
        return True # Finished Range
                
    except Exception as e:
        print(f"[!] Story processing error: {e}")
        return "RESTART"
    finally:
        if conn: conn.close()

def process_repair_story(driver, story_data, acc_idx=0):
    story_id, slug, title, url, downloaded_idx, total_db, status = story_data
    
    conn = get_db_connection()
    if not os.path.exists(IMPORT_DIR): os.makedirs(IMPORT_DIR, exist_ok=True)
    story_dir = os.path.join(IMPORT_DIR, slug)
    if not os.path.exists(story_dir): os.makedirs(story_dir)
    
    print(f"[*] REPAIR MODE: {title}")
    
    # 1. Scan files for INDICES
    existing_indices = set()
    files = os.listdir(story_dir)
    for f in files:
        # Match pattern: ..._(\d+).txt
        match = re.search(r'_(\d{4})\.txt$', f)
        if match:
            existing_indices.add(int(match.group(1)))
            
    # Need total chapters. Check page or DB?
    # Go to page first.
    read_url = url.replace('/sach/', '/read/')
    driver.get(read_url); time.sleep(5)
    ensure_login_session(driver, acc_idx)
    handle_popups(driver)
    
    _, total_footer = get_footer_index(driver)
    total_real = total_footer or total_db or 0
    
    print(f"  [i] Total Real: {total_real}. Existing: {len(existing_indices)}")
    
    missing = []
    for i in range(1, total_real + 1):
        if i not in existing_indices:
            missing.append(i)
            
    print(f"  [!] Missing {len(missing)} chapters: {missing[:10]}...")
    
    if not missing:
        cursor = conn.cursor()
        cursor.execute("UPDATE stories SET crawl_status = 'paused' WHERE id = %s", (story_id,))
        conn.close()
        return

    # Repair Loop
    last_content_snippet = None
    for idx in missing:
        if os.path.exists('stop.signal'): break
        print(f"  [>] Repairing Index {idx}...")
        
        if not smart_navigate_absolute(driver, idx, menu_map=menu_map):
            print("  [!] Skip due to nav fail.")
            continue
            
        # Scrape inline
        try:
             WebDriverWait(driver, 30).until(EC.presence_of_element_located((By.CSS_SELECTOR, "div.space-y-\\[30px\\]")))
             driver.execute_script("window.scrollTo(0, document.body.scrollHeight/5);")
             time.sleep(0.5)
             
             content_text = ""
             cur_snippet = ""
             
             # Retry Loop for Content
             for attempt in range(10):
                 soup = BeautifulSoup(driver.page_source, 'html.parser')
                 
                 content_div = soup.find('div', class_=lambda x: x and 'space-y-[30px]' in x)
                 if content_div:
                     raw_text = content_div.get_text("\n").strip()
                     
                     if len(raw_text) < 50:
                         print("  [.] Content too short. Waiting...")
                         time.sleep(3)
                         continue
                         
                     # Similarity Check: Compare First 100 Chars (Strict)
                     cur_snippet = raw_text[:100]
                     
                     # DEBUG LOGGING (Repair)
                     try:
                        with open("debug_lag_log.txt", "a", encoding="utf-8") as f:
                            timestamp = time.strftime("%H:%M:%S")
                            f.write(f"[REPAIR {timestamp}] Attempt {attempt+1}/10 (Target: {idx})\n")
                            f.write(f"  PREV: {('None' if last_content_snippet is None else last_content_snippet)}\n")
                            f.write(f"  CURR: {cur_snippet}\n")
                            match = (last_content_snippet and cur_snippet == last_content_snippet)
                            f.write(f"  MATCH: {match}\n\n")
                     except: pass
                     
                     if last_content_snippet and cur_snippet == last_content_snippet:
                          print(f"  [!] Lag Detected (Repair): Matches previous! (Attempt {attempt+1})")
                          
                          if attempt == 0:
                              print(f"  [!] Layer 1: Immediate Refresh...")
                              driver.refresh(); time.sleep(5)
                              ensure_login_session(driver, acc_idx)
                          else:
                              print(f"  [!] Layer 2: Forcing Menu Jump to {idx}...")
                              smart_navigate_absolute(driver, idx, force_reload=True)
                              
                          continue
                          
                     content_text = raw_text
                     last_content_snippet = cur_snippet
                     break
                 else:
                     time.sleep(2)

             if not content_text:
                 print("  [!] Failed to extract repair content.")
                 continue

             # Got content, proceed to save
             soup = BeautifulSoup(driver.page_source, 'html.parser') # re-soup just in case
             t_div = soup.find('div', class_='text-center font-bold')
             tit = t_div.get_text().strip() if t_div else "Chapter"
             
             # Verify index again
             curr, _ = get_footer_index(driver)
             final_idx = curr if curr else idx
             
             safe = re.sub(r'[\\/*?:"<>|]', "", tit).strip()
             fname = f"{safe}_{final_idx:04d}.txt"
             
             with open(os.path.join(story_dir, fname), "w", encoding="utf-8") as f:
                 f.write(f"{tit}\nIndex:{final_idx}\n\n{content_text}")
             print(f"  [+] Repaired: {fname}")
        except Exception as e:
            print(f"  [!] Error repair: {e}")
            
    conn.close()

def main():
    os.system("title pd_scraper_fast-v1.py")
    
    parser = argparse.ArgumentParser(description='PD Scraper Fast V1 (Distributed)')
    parser.add_argument('account_idx', type=int, nargs='?', default=0, help='Account Index (default: 0)')
    parser.add_argument('--admin', type=str, default=None, help='Admin name for task filtering')
    args = parser.parse_args()
    
    acc_idx = args.account_idx
    admin_name = args.admin
    
    print(f"[*] Starting PD Scraper (Staggered Startup V2)...")
    print(f"[*] Account: {acc_idx}")
    print(f"[*] Admin:   {admin_name if admin_name else 'NONE (Open Mode)'}")
        
    driver = None
    user_data_dir = None
    conn = None
        
    try:
        # 1. Acquire Lock FIRST (Before opening Chrome)
        print(f"[*] Thread {acc_idx}: Requesting startup slot...")
        acquire_startup_lock()
        
        # 2. Setup Driver (Only after lock is acquired)
        print(f"[*] Thread {acc_idx}: Starting Chrome...")
        driver, user_data_dir = setup_driver(acc_idx)
        
        # 3. Login
        while True:
            if login_procedure(driver, acc_idx): 
                print("[*] Initial Login Complete. Entering Task Loop...")
                break
            print("[!] Initial Login Failed. Retrying in 60s...")
            time.sleep(60)
            
        while True:
            story = claim_next_story(acc_idx, admin_name)
            if not story:
                print(f"  [.] No tasks found. Waiting 10s... (Active threads: {acc_idx})", end='\r')
                time.sleep(10)
                continue
            
            print(f"\n[*] Main Loop: Claimed Story ID {story[0]} - {story[2]} (Link: {story[3]})")
                
            # INNER LOOP: Retry same story if RESTART is signaled
            try:
                current_story = story
                restart_count = 0
                while True:
                    try:
                        if current_story[6] == 'repairing':
                            process_repair_story(driver, current_story, acc_idx)
                            break # Repair done (or failed inside), break to claim next
                        else:
                            result = process_story(driver, current_story, acc_idx)
                            
                            if result == "RESTART":
                                 restart_count += 1
                                 if restart_count >= 3:
                                     raise Exception(f"Max restarts (3) exceeded for Story {current_story[0]}. Skipping.")
                                     
                                 print(f"[*] Thread {acc_idx}: RESTART TRIGGERED for Story {current_story[0]}.")
                                 try: driver.quit()
                                 except: pass
                                 
                                 print(f"[*] Thread {acc_idx}: Re-launching Chrome...")
                                 driver, user_data_dir = setup_driver(acc_idx)
                                 
                                 # Re-login
                                 while True:
                                    if login_procedure(driver, acc_idx): break
                                    time.sleep(30)
                                    
                                 # DO NOT break. Continue inner loop to retry process_story(current_story)
                                 print(f"[*] Thread {acc_idx}: Resuming Story {current_story[0]}...")
                                 continue 
                                 
                            break # Success or Skip -> Break Inner Loop to claim next
                    except Exception as e:
                         # Treat inner error as fatal for story? Or re-raise to outer?
                         # If we break here, we go to outer except? No, we just break loop.
                         # Better re-raise to hit the outer except which logs logic error
                         raise e
            except Exception as e:
                print(f"[!] Error: {e}")
                update_story_data(story[0], error=str(e))
                
                # Restart driver
                if driver: driver.quit()
                if user_data_dir: 
                    # DO NOT DELETE user_data_dir if it's persistent profile
                    pass 
                
                # Throttle restart to avoid rapid loop
                time.sleep(5) 
                
                # Re-acquire lock for restart? 
                # Ideally yes, to prevent restart-storm.
                acquire_startup_lock()
                driver, user_data_dir = setup_driver(acc_idx)
                
                while True:
                    if login_procedure(driver, acc_idx): break
                    time.sleep(60)
                    
    except KeyboardInterrupt:
        print("[!] Stopped.")
    except Exception as e:
        print(f"\n[!] LỖI KHỞI ĐỘNG SCRAPER: {e}")
        import traceback
        traceback.print_exc()
        input("\nNhấn Enter để đóng cửa sổ...")
    finally:
        if 'conn' in locals() and conn: conn.close()
        
        # Always try to release lock at exit just in case we held it
        release_startup_lock()
        
        if driver: 
            try: driver.quit()
            except: pass
        if user_data_dir: 
            try: shutil.rmtree(user_data_dir, ignore_errors=True)
            except: pass

if __name__ == "__main__":
    main()
