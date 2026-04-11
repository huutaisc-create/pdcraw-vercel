# gemini_meta_tool.py - Tạo tên truyện / Synopsis / Cover prompt qua Gemini
# Dựa trên gemini_tool_docker.py, chỉ thay rule block và logic đọc input
import sys
import io
import platform
import os
import subprocess

# --- UTF-8 stdout ---
if hasattr(sys.stdout, 'buffer'):
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='replace')

IS_LINUX   = platform.system() == "Linux"
IS_WINDOWS = platform.system() == "Windows"

# =========================================================================================
# DISPLAY ISOLATION (giữ nguyên từ bản gốc)
# =========================================================================================
if IS_LINUX:
    DISPLAY_NUM = os.environ.get("DISPLAY_NUM", "99")
    os.environ["DISPLAY"] = f":{DISPLAY_NUM}"
    print(f"[INFO] Linux mode · DISPLAY=:{DISPLAY_NUM}")
    os.environ["PYPERCLIP_COPY_COMMAND"]  = "xclip -selection clipboard"
    os.environ["PYPERCLIP_PASTE_COMMAND"] = "xclip -selection clipboard -o"

import pyautogui
import pyperclip
import time
import random
import json
import re

try:
    pyperclip.copy("__clipboard_test__")
    assert pyperclip.paste() == "__clipboard_test__"
    pyperclip.copy("")
    print("[INFO] Clipboard OK")
except Exception as e:
    print(f"[WARN] Clipboard test fail: {e}")

pyautogui.FAILSAFE = False
pyautogui.PAUSE = 0.9

if IS_WINDOWS:
    import ctypes
    try:
        ctypes.windll.shcore.SetProcessDpiAwareness(2)
    except Exception:
        try: ctypes.windll.shcore.SetProcessDpiAwareness(1)
        except Exception:
            try: ctypes.windll.user32.SetProcessDPIAware()
            except Exception: print("[WARN] Could not set DPI Awareness")
else:
    print("[INFO] DPI Awareness skipped (Linux)")

BASE_DIR   = os.path.dirname(os.path.abspath(__file__))
IMAGES_DIR = os.path.join(BASE_DIR, 'images')

def get_path(filename):
    return os.path.join(IMAGES_DIR, filename)

# =========================================================================================
# PHẦN 1: DỮ LIỆU PROMPT & CONSTANTS
# =========================================================================================

GREETINGS_LIST = [
    "Chào bạn, chúc bạn một ngày tốt lành.", "Xin chào, hôm nay trời đẹp nhỉ.", "Hello, chúc bạn luôn vui vẻ.",
    "Chào bạn hiền, rất vui được gặp lại.", "Hi there, mong bạn có nhiều năng lượng.", "Chào, chúc mọi việc suôn sẻ.",
    "Xin chào, hy vọng bạn đang thấy thoải mái.", "Hello, chúc bạn làm việc hiệu quả.", "Chào bạn, ngày mới an lành nhé.",
    "Hi, rất vui vì bạn ở đây.", "Chào nhé, chúc vạn sự như ý.", "Hello friend, chúc bạn luôn mạnh khỏe.",
    "Xin chào, nay là một ngày tuyệt vời.", "Chào bạn, chúc bạn luôn rạng rỡ.", "Hi, chúc buổi làm việc thuận lợi.",
]

PRAISES_LIST = [
    "Cảm ơn bạn, bản trước rất tốt.", "Bạn làm tuyệt lắm, cảm ơn nhé.", "Rất tốt, cảm ơn bạn nhiều.",
    "Tuyệt vời, mình rất thích kết quả vừa rồi.", "Cảm ơn bạn, đúng phong cách lắm.", "Bạn làm rất nhanh và sáng tạo, cảm ơn.",
    "Good job, cảm ơn bạn nhé.", "Rất hài lòng với kết quả, cảm ơn bạn.", "Bạn phân tích rất hay, cảm ơn.",
    "Cảm ơn nha, đúng ý mình rồi.", "Tuyệt quá, cảm ơn bạn nhiều lắm.",
]

GOODBYES_LIST = [
    "Cảm ơn bạn rất nhiều, tạm biệt nhé.", "Cảm ơn bạn đã giúp đỡ, hẹn gặp lại.",
    "Tuyệt vời, cảm ơn bạn. Chúc bạn ngày tốt lành.", "Cảm ơn nhiều, tạm biệt bạn.",
]

# ── Rule block chính (gửi 1 lần khi mở session mới) ─────────────────────────
META_RULE_BLOCK = """Dựa trên tệp tin danh sách chương mà tôi đã gửi hãy:

1. Sáng tạo tên truyện: Đưa ra 3 phương án:
   - 01 tên Hán Việt đúng tinh thần nguyên tác.
   - 01 tên thuần Việt lôi cuốn, dễ hiểu.
   - 01 tên mang tính gây tò mò cao (Click-bait) cho độc giả hiện đại.

2. Hãy đóng vai một biên tập viên tiểu thuyết mạng chuyên nghiệp. Dựa trên thông tin truyện tôi cung cấp, hãy viết một đoạn Văn án (Blurb/Teaser) để thu hút độc giả.
    Cấu trúc bắt buộc:
    Câu mồi (Hook): Một câu khẳng định hoặc một câu thoại đắt giá nhất của nhân vật chính.
    Đoạn dẫn: Mô tả ngắn gọn tình huống kịch tính nhất hoặc mâu thuẫn cốt lõi (không quá 3 câu).
    Thiết lập: Liệt kê các thuộc tính nhân vật (ví dụ: Ma tôn x Tiên tôn, Phúc hắc x Ngạo kiều).
    Hệ thống Tag: Liệt kê các từ khóa thể loại dưới dạng #Tag.
    Lưu ý: viết 3 văn án với 3 phong cách sau: Phong cách Kịch tính - Bí ẩn; Phong cách vô địch lưu - sảng; Phong cách Hán Việt - Trang trọng phù hợp với tinh thần nguyên tác.

3. Tự chọn Phong cách Ảnh bìa: Dựa vào bảng gợi ý các phong cách dưới đây, hãy chọn ra 03 phong cách phù hợp nhất với linh hồn của tác phẩm này:
   [Webnovel Premium | Anime Movie | Gothic Noir | Glitch Art | Ink Wash | Wuxia Epic | Dark Fantasy | Cultivation Art | Neon Cyberpunk | Classic Manhua | ultra-photorealistic |hyper-realistic ]
   
(Viết 3 prompt ) Với mỗi phong cách bạn chọn được hãy Viết một đoạn Prompt tiếng Anh với yêu cầu sau:

(Viết 3 prompt ) Với mỗi phong cách bạn chọn được hãy Viết một đoạn Prompt tiếng Anh với yêu cầu sau: nhân vật chính, trẻ đẹp cuốn hút, cận cảnh (mid-shot nửa người từ mông trở lên),  bám sát truyện, mô tả kỹ từng chi tiết kiểu tóc và trang phục, nhân vật lúc nào cũng đẹp ngời ngời, tóc bay, chọn bối cảnh phù hợp với tinh thần và nội dung truyện, không text, không yếu tố chính trị, không bạo lực máu me, 9:16

Yêu cầu về ngôn ngữ:
- Không dùng tên nhân vật cho mô tả, tên truyện.
- Không có yếu tố: chính trị, máu me, bạo lực.
- Giữ đúng cách xưng hô [Ví dụ: Ta - Ngươi] và các thuật ngữ chuyên môn về cấp độ/chiêu thức có trong danh sách."""

# ── Prompt khi tiếp tục session (story thứ 2 trở đi) ────────────────────────
META_SUBSEQUENT_LIST = [
    "Bạn hãy làm tiếp truyện sau nhé, vẫn giữ đúng format 3 phần (Tên truyện / Synopsis / Image Prompt) và các quy tắc đã gửi.",
    "Chúng ta sang truyện tiếp theo nào, bạn áp dụng đúng các luật đã lưu và format như trước nhé.",
    "Phiền bạn phân tích truyện này theo đúng các yêu cầu ban đầu, giữ nguyên cấu trúc 3 phần.",
    "Mời bạn làm tiếp truyện mới nhé, nhớ giữ đúng phong cách và format đã định.",
    "Bạn hãy tiếp tục với truyện sau đi, vẫn tuân thủ nghiêm ngặt các hướng dẫn, giữ nguyên format.",
    "Nhờ bạn phân tích truyện sau theo các quy tắc đã gửi, xuất ra đúng 3 phần như trước.",
    "Chúng mình làm tiếp truyện kế nhé, vẫn theo các tiêu chuẩn cũ và giữ nguyên cấu trúc output.",
    "Bạn xử lý truyện này giúp mình nha, nhớ bám sát các rule và format 3 phần.",
    "Sang truyện mới rồi, bạn hãy phân tích theo đúng yêu cầu cũ và giữ nguyên format nhé.",
    "Nhờ bạn làm tiếp truyện kế tiếp, vẫn giữ đúng các quy tắc và format đã định.",
]

# =========================================================================================
# PHẦN 2: HÀM HỖ TRỢ ĐỌC all.txt + meta.json
# =========================================================================================

def build_story_prompt(story_dir: str) -> str:
    """Tạo prompt gửi Gemini: chỉ tên truyện + nội dung all.txt."""
    # Đọc tên truyện từ meta.json nếu có, fallback tên thư mục
    meta_path = os.path.join(story_dir, 'meta.json')
    title = os.path.basename(story_dir)
    if os.path.exists(meta_path):
        try:
            with open(meta_path, encoding='utf-8') as f:
                title = json.load(f).get('original_title', title) or title
        except Exception:
            pass

    # Đọc all.txt
    all_txt_path = os.path.join(story_dir, 'all.txt')
    with open(all_txt_path, encoding='utf-8') as f:
        all_txt_content = f.read().strip()

    prompt = f"Tên truyện: {title}\n\n{all_txt_content}"
    return prompt, title


def scan_stories(parent_dir: str, force: bool = False) -> list:
    """
    Quét parent_dir, tìm subdirectory chứa all.txt mà chưa có gemini-temp.txt.
    Nếu force=True thì xử lý lại kể cả đã có gemini-temp.txt.
    Trả về list of story_dir (string).
    """
    result = []
    if not os.path.isdir(parent_dir):
        print(f"[LỖI] Không tìm thấy thư mục: {parent_dir}")
        return result

    entries = sorted(os.listdir(parent_dir))
    for entry in entries:
        story_dir   = os.path.join(parent_dir, entry)
        if not os.path.isdir(story_dir):
            continue
        all_txt     = os.path.join(story_dir, 'all.txt')
        result_path = os.path.join(story_dir, 'gemini-temp.txt')
        if not os.path.exists(all_txt):
            continue
        if os.path.exists(result_path) and not force:
            continue  # Đã xử lý rồi, bỏ qua
        result.append(story_dir)

    return result


# =========================================================================================
# PHẦN 3–6: GIỮ NGUYÊN TỪ gemini_tool_docker.py
# (display helpers, clipboard, image matching, Gemini interaction)
# =========================================================================================

def get_current_display():
    return os.environ.get("DISPLAY", ":99")

def verify_display():
    display = get_current_display()
    try:
        result = subprocess.run(
            ["xdpyinfo", "-display", display],
            capture_output=True, timeout=5
        )
        if result.returncode == 0:
            print(f"[INFO] Display {display} OK ✓")
            return True
        else:
            print(f"[WARN] Display {display} không phản hồi! Xvfb chưa chạy?")
            return False
    except Exception as e:
        print(f"[WARN] Không kiểm tra được display: {e}")
        return False

def get_chrome_window_id():
    display = get_current_display()
    try:
        result = subprocess.run(
            ["xdotool", "search", "--onlyvisible", "--name", "Gemini"],
            capture_output=True, text=True, timeout=5, env=os.environ
        )
        ids = [i for i in result.stdout.strip().split('\n') if i.strip()]
        if ids: return ids[-1]
    except Exception as e:
        print(f"   [WinID] Lỗi: {e}")
    return None

def ensure_browser_focused():
    if not IS_LINUX: return True
    display = get_current_display()
    print(f"   [Focus] Ensuring Chrome focus on {display}...", end="")
    window_id = get_chrome_window_id()
    if window_id:
        try:
            subprocess.run(["wmctrl", "-ir", window_id, "-b", "add,maximized_vert,maximized_horz"], env=os.environ, check=False)
            subprocess.run(["xdotool", "windowactivate", "--sync", window_id], env=os.environ, check=False)
            subprocess.run(["xdotool", "windowfocus", "--sync", window_id], env=os.environ, check=False)
            time.sleep(0.4)
            print(f" OK (WinID={window_id})")
            return True
        except Exception as e:
            print(f" Lỗi wmctrl/xdotool: {e}")
    else:
        try:
            subprocess.run(["wmctrl", "-a", "Gemini"], env=os.environ, check=False)
            time.sleep(0.4)
            print(" OK (wmctrl fallback)")
            return True
        except Exception as e:
            print(f" Fallback cũng lỗi: {e}")
    return False

def copy_to_clipboard_verified(text):
    for attempt in range(3):
        try:
            if IS_LINUX:
                proc = subprocess.Popen(['xclip', '-selection', 'clipboard'], stdin=subprocess.PIPE, env=os.environ)
                proc.communicate(input=text.encode('utf-8'))
                time.sleep(0.5)
                result = subprocess.check_output(['xclip', '-selection', 'clipboard', '-o'], env=os.environ).decode('utf-8')
                if result.strip() == text.strip(): return True
            else:
                pyperclip.copy(text)
                time.sleep(0.5)
                if pyperclip.paste().strip() == text.strip(): return True
        except Exception as e:
            print(f"   [Clipboard] Attempt {attempt+1} fail: {e}")
        time.sleep(0.5)
    return False

def smart_click(x, y):
    for i in range(3):
        pyautogui.moveTo(x, y)
        time.sleep(0.1)
        cur_x, cur_y = pyautogui.position()
        if abs(cur_x - x) < 5 and abs(cur_y - y) < 5:
            pyautogui.click()
            return True
    pyautogui.click(x, y)
    return False

def safe_locate(image_path, confidence=0.8, grayscale=True):
    full_path = get_path(image_path)
    if not os.path.exists(full_path): return None
    try:
        return pyautogui.locateOnScreen(full_path, confidence=confidence, grayscale=grayscale)
    except pyautogui.ImageNotFoundException:
        return None
    except Exception as e:
        print(f"   [DEBUG safe_locate] {type(e).__name__}: {e}")
        return None

def find_and_click_image(image_names, description="target"):
    print(f"   [Scan] Tìm {description}...", end="")
    full_paths = [get_path(img) for img in image_names]
    for i in range(10):
        for img_path in full_paths:
            if os.path.exists(img_path):
                try:
                    pos = pyautogui.locateCenterOnScreen(img_path, confidence=0.8, grayscale=True)
                    if pos:
                        print(f" -> Found {os.path.basename(img_path)}")
                        pyautogui.click(pos)
                        time.sleep(0.5)
                        return True
                except pyautogui.ImageNotFoundException:
                    pass
                except Exception as e:
                    print(f"   [DEBUG] {type(e).__name__}: {e}")
        time.sleep(1.0)
        print(".", end="", flush=True)
    print(f"\n   [LỖI] Không tìm thấy ảnh cho {description}!")
    return False

def click_and_paste_dynamic(text):
    ensure_browser_focused()
    input_icons = ["input_icon_dark.png", "input_icon.png"]
    if not find_and_click_image(input_icons, "Ô Nhập Liệu"):
        return False
    time.sleep(0.5)
    if not copy_to_clipboard_verified(text):
        print("   [LỖI] Clipboard fail.")
    pyautogui.hotkey('ctrl', 'v')
    time.sleep(0.5)
    return True

def wait_for_page_load(timeout=60):
    print("   [Page Load] Waiting for Gemini Logo...", end="")
    start = time.time()
    logo_files = ["gemini_logo.png", "gemini_logo_dark.png", "gemini_logo.PNG", "gemini_logo_dark.PNG"]
    while True:
        try:
            for f in logo_files:
                if safe_locate(f, confidence=0.8, grayscale=True):
                    print(f"\n   -> Loaded: {f} ({time.time()-start:.1f}s)")
                    return True
        except: pass
        if time.time() - start > timeout:
            print(f"\n   [Timeout] Load lâu quá -> Continue.")
            return False
        time.sleep(1.0)
        print(".", end="", flush=True)

def wait_for_reply(timeout=180):
    print("   [Reply] Waiting for Finish Icon...", end="")
    time.sleep(5)
    print("   [Wait] Sleeping 10s before searching...", end="")
    time.sleep(10)
    print(" Done.")

    toolbar_variants = [
        get_path("copy-here-dark.PNG"), get_path("copy-here.png"),
        get_path("copy-here-dark.png"), get_path("copy-here.PNG")
    ]
    copy_btn_variants = [
        get_path("copy_icon_dark.png"), get_path("copy_icon.png"),
        get_path("copy_icon_dark.PNG"), get_path("copy_icon.PNG")
    ]

    start = time.time()
    ensure_browser_focused()
    pyautogui.moveRel(0, -200)
    time.sleep(0.5)

    while True:
        found_toolbar = False
        for img_path in toolbar_variants:
            if os.path.exists(img_path):
                toolbar_loc = safe_locate(img_path, confidence=0.8, grayscale=True)
                if toolbar_loc:
                    print(f"\n   -> Found Toolbar: {os.path.basename(img_path)}!")
                    found_toolbar = True
                    break
        if found_toolbar:
            for copy_img in copy_btn_variants:
                if os.path.exists(copy_img):
                    copy_loc = safe_locate(copy_img, confidence=0.8, grayscale=True)
                    if copy_loc:
                        print("   -> Found Copy Button!")
                        return True
            print("   [WARN] Found Toolbar but not Copy Button? Retrying...")
        print("   [Scroll] Searching...", end="")
        pyautogui.scroll(-500)
        if time.time() - start > timeout:
            print(f"\n   [Timeout] Quá {timeout}s không thấy Toolbar.")
            return False
        time.sleep(0.5)
        print(".", end="", flush=True)

def click_like_button_if_lucky(lucky_chance=0.1):
    if random.random() > lucky_chance: return
    print("   [LUCKY] Đang tìm nút Like...", end="")
    like_files = ["like_icon.png", "like_icon_dark.png"]
    try:
        btn = None
        for f in like_files:
            btn = safe_locate(f, confidence=0.8, grayscale=True)
            if btn: break
        if btn:
            pyautogui.click(pyautogui.center(btn))
            print(" -> Đã bấm Like!")
            time.sleep(0.5)
        else:
            print(" -> Không thấy nút Like, bỏ qua.")
    except:
        print(" -> Lỗi khi tìm Like.")

def check_copy_button_and_get_text():
    t0 = time.time()
    ensure_browser_focused()
    input_icons = ["input_icon_dark.png", "input_icon.png"]
    found_input = False
    for icon in input_icons:
        if safe_locate(icon):
            try:
                pos = pyautogui.locateCenterOnScreen(get_path(icon), confidence=0.8, grayscale=True)
                if pos:
                    pyautogui.click(pos)
                    found_input = True
                    pyautogui.moveTo(pos.x, pos.y - 400)
                    break
            except: pass
    if not found_input:
        pyautogui.click(pyautogui.size()[0]//2, pyautogui.size()[1]//2)

    time.sleep(0.5)
    btn = None
    icon_variants = [
        get_path("copy_icon_dark.png"), get_path("copy_icon.png"),
        get_path("copy_icon_dark.PNG"), get_path("copy_icon.PNG")
    ]
    print(f"\n   [{time.time()-t0:.1f}s] Scanning & Scrolling (Timeout 3m)...")
    start_search = time.time()
    while time.time() - start_search < 180:
        try:
            for icon_path in icon_variants:
                if os.path.exists(icon_path):
                    btn = safe_locate(icon_path, confidence=0.8, grayscale=True)
                    if btn: break
            if btn: break
        except: pass
        pyautogui.scroll(-2000)
        time.sleep(0.5)
        print(".", end="", flush=True)

    if not btn:
        print("\n   [LỖI] Không tìm thấy nút Copy!")
        return None

    print(f"\n   -> Found Copy Button! Clicking...")
    pyautogui.click(pyautogui.center(btn))
    time.sleep(0.8)

    if IS_LINUX:
        try:
            content = subprocess.check_output(
                ['xclip', '-selection', 'clipboard', '-o'], env=os.environ
            ).decode('utf-8').strip()
        except Exception as e:
            print(f"   [LỖI] Đọc clipboard fail: {e}")
            content = ""
    else:
        content = pyperclip.paste().strip()
    return content

def perform_reset_sequence():
    print("   [RESET] Refreshing page and starting new session...")
    ensure_browser_focused()
    pyautogui.hotkey('f5')
    time.sleep(10)
    wait_for_page_load(timeout=15)
    print("   -> Creating new chat (Click Icon)...")
    new_chat_icons = ["new_chat_icon_dark.png", "new_chat_icon.png"]
    if not find_and_click_image(new_chat_icons, "Nút New Chat"):
        print("   [WARN] Không thấy nút New Chat. Thử phím tắt...")
        pyautogui.hotkey('ctrl', 'shift', 'o')
    time.sleep(4)

# =========================================================================================
# MAIN FLOW
# =========================================================================================

def prompt_folder(label):
    """Nhập đường dẫn thư mục, nhận đường dẫn nguyên bản."""
    # Docker/Linux: bỏ ghi chú bên dưới để bật convert ổ đĩa Windows → /mnt/X
    # drive_map = {"e:": "/mnt/e", "d:": "/mnt/d", "c:": "/mnt/c", "f:": "/mnt/f"}
    print(f"\n{'='*50}")
    print(f"  Nhập đường dẫn {label}")
    print(f"  Ví dụ: D:\\Webtruyen\\pdcraw\\data_import")
    print(f"{'='*50}")
    while True:
        path = input("  >> ").strip().strip('"').strip("'")
        if os.path.isdir(path):
            print(f"   [OK] ✓ Đã nhận diện: {path}")
            return path
        print(f"   [LỖI] Không tìm thấy thư mục: '{path}'")

def main():
    worker_id = os.environ.get("DISPLAY_NUM", "99")
    force     = '--force' in sys.argv

    print("=" * 55)
    print(f"  GEMINI META TOOL (Worker :{worker_id})")
    print("  Tạo tên truyện / Synopsis / Cover Prompt")
    if force:
        print("  [!] Chế độ --force: ghi đè gemini-temp.txt đã có")
    print("=" * 55)

    if IS_LINUX:
        verify_display()

    required_imgs = ["gemini_logo_dark.png", "copy_icon_dark.png"]
    missing = [f for f in required_imgs if not os.path.exists(get_path(f))]
    if missing:
        print(f"[WARN] Thiếu file ảnh: {missing}")
        time.sleep(2)

    IMPORT_DIR = prompt_folder("thư mục chứa các folder truyện")

    # Quét các folder có all.txt nhưng chưa có gemini-temp.txt (trừ khi --force)
    stories = scan_stories(IMPORT_DIR, force=force)
    print(f"\nTìm thấy {len(stories)} truyện cần xử lý.")

    if not stories:
        print("[INFO] Không có truyện nào cần xử lý. Thoát.")
        return

    for story_dir in stories:
        name = os.path.basename(story_dir)
        has_result = os.path.exists(os.path.join(story_dir, 'gemini-temp.txt'))
        tag = " [ghi đè]" if has_result and force else ""
        print(f"   - {name}{tag}")

    print("\n" + "=" * 50)
    print("  Hãy chuyển sang tab Gemini trên Chrome.")
    print("  Nhấn ENTER để bắt đầu (tool sẽ chờ 5 giây)...")
    print("=" * 50)
    input()
    print("Bắt đầu sau 5 giây...")
    time.sleep(5)

    session_items_count = 0
    MAX_ITEMS_PER_SESSION = 8
    is_new_session = True

    perform_reset_sequence()

    for i, story_dir in enumerate(stories):
        result_path = os.path.join(story_dir, 'gemini-temp.txt')

        # Double-check nếu không force
        if not force and os.path.exists(result_path):
            print(f"\n=== SKIP (đã có gemini-temp.txt): {os.path.basename(story_dir)} ===")
            continue

        try:
            story_content, title = build_story_prompt(story_dir)
        except Exception as e:
            print(f"\n=== LỖI đọc all.txt: {os.path.basename(story_dir)} — {e} ===")
            continue

        print(f"\n=== Đang xử lý: {title} ({i+1}/{len(stories)}) ===")

        max_retries = 3
        retry_count = 0

        while retry_count < max_retries:
            try:
                if session_items_count >= MAX_ITEMS_PER_SESSION:
                    print("   [INFO] Đủ session items. Reset session mới.")
                    perform_reset_sequence()
                    session_items_count = 0
                    is_new_session = True

                if is_new_session:
                    greet = random.choice(GREETINGS_LIST)
                    final_prompt = f"{greet}\n\n{META_RULE_BLOCK}\n\n{story_content}"
                    print("   -> Mode: NEW SESSION (Greeting + Rules + Story)")
                else:
                    praise = random.choice(PRAISES_LIST)
                    subsequent = random.choice(META_SUBSEQUENT_LIST)
                    final_prompt = f"{praise}\n\n{subsequent}\n\n{story_content}"
                    print("   -> Mode: FOLLOW-UP (Praise + Prompt + Story)")

                if not click_and_paste_dynamic(final_prompt):
                    print("   [LỖI] Không tìm thấy ô nhập liệu. Retrying...")
                    raise Exception("Input Field Not Found")

                wait_time = random.uniform(0, 2)
                print(f"   [Human-like] Waiting {wait_time:.1f}s before sending...")
                time.sleep(wait_time)

                pyautogui.press('enter')
                time.sleep(3)

                if not wait_for_reply(timeout=240):
                    print("\n   [TIMEOUT] Quá 4 phút không thấy Finish Icon.")
                    raise Exception("Timeout Response")

                print("   -> Reply Finished. Getting content...")
                result_text = check_copy_button_and_get_text()

                if not result_text:
                    raise Exception("Copy failed (No button or empty clipboard)")

                if len(result_text) < 150:
                    print(f"   [WARN] Kết quả quá ngắn ({len(result_text)} chars).")
                    raise Exception("Result too short (Blocked?)")

                # Lưu gemini-temp.txt — giữ nguyên format Gemini trả về
                import datetime
                with open(result_path, 'w', encoding='utf-8') as f:
                    f.write(f"# Story: {title}\n")
                    f.write(f"# Generated: {datetime.datetime.now().isoformat()}\n\n")
                    f.write(result_text)

                print(f"   [SAVED] gemini-temp.txt → {story_dir}")

                click_like_button_if_lucky(0.15)

                session_items_count += 1
                is_new_session = False
                break

            except Exception as e:
                retry_count += 1
                print(f"\n   [ERROR] {e} -> Retry ({retry_count}/{max_retries})")
                if retry_count >= max_retries:
                    print("   [SKIP] Quá số lần thử lại. Bỏ qua truyện này.")
                    err_log = os.path.join(BASE_DIR, "meta_error_log.txt")
                    with open(err_log, "a", encoding="utf-8") as ef:
                        ef.write(f"[Worker:{worker_id}] {title}: Fail after 3 retries. Error: {e}\n")
                else:
                    perform_reset_sequence()
                    is_new_session = True

    # Kết thúc session
    bye = random.choice(GOODBYES_LIST)
    try:
        click_and_paste_dynamic(bye)
        pyautogui.press('enter')
        time.sleep(2)
    except: pass

    print("\n" + "=" * 55)
    print(f"  Hoàn thành. Đã xử lý {len(stories)} truyện.")
    print("=" * 55)


if __name__ == '__main__':
    main()
