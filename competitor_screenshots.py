import io
import os
import sys
import time
import requests
from pathlib import Path
from PIL import Image
from playwright.sync_api import sync_playwright

COMPETITORS = [
    ("Scale AI", "https://scale.com"),
    ("Outlier AI", "https://outlier.ai"),
    ("Mindrift", "https://mindrift.ai"),
    ("micro1", "https://www.micro1.ai"),
    ("Turing", "https://www.turing.com"),
    ("DataAnnotation", "https://dataannotation.tech"),
    ("Alignerr", "https://www.alignerr.com"),
    ("Handshake AI", "https://www.handshake.ai"),
    ("Invisible Technologies", "https://invisibletech.ai"),
    ("Surge AI", "https://surgehq.ai"),
]

NOTION_PAGE_ID = "3665392cc93e80cf88f4f2217e9a56e4"

OUT_DIR = Path("/Users/chasegladden/mercor-reporting/competitor-screenshots")
OUT_DIR.mkdir(exist_ok=True)


VIEWPORT_W = 1440
VIEWPORT_H = 900


def take_screenshot(page, url, path):
    try:
        page.goto(url, wait_until="networkidle", timeout=30000)
    except Exception:
        try:
            page.goto(url, wait_until="domcontentloaded", timeout=20000)
            time.sleep(3)
        except Exception as e:
            print(f"  Failed to load {url}: {e}")
            return False

    time.sleep(2)

    # Dismiss cookie banners
    for selector in [
        "button[id*='accept']", "button[class*='accept']",
        "button[id*='cookie']", "button[class*='cookie']",
        "button:has-text('Accept')", "button:has-text('Accept all')",
        "button:has-text('Accept Cookies')", "button:has-text('I Accept')",
        "button:has-text('Got it')", "button:has-text('OK')",
        "button:has-text('Agree')", "[aria-label='Accept cookies']",
    ]:
        try:
            btn = page.locator(selector).first
            if btn.is_visible(timeout=500):
                btn.click()
                time.sleep(0.5)
                break
        except Exception:
            pass

    time.sleep(1)

    # Freeze CSS animations
    page.add_style_tag(content=(
        "*, *::before, *::after {"
        "  animation-duration: 0.001s !important;"
        "  animation-delay: 0s !important;"
        "  transition-duration: 0.001s !important;"
        "  transition-delay: 0s !important;"
        "}"
    ))

    page.screenshot(path=str(path), full_page=False)
    print(f"  Screenshot saved: {path.name} ({path.stat().st_size // 1024}KB)")
    return True


def upload_image(path):
    """Upload to freeimage.host and return the permanent public URL."""
    with open(path, "rb") as f:
        resp = requests.post(
            "https://freeimage.host/api/1/upload",
            data={"key": "6d207e02198a847aa98d0a2a901485a5", "action": "upload", "format": "json"},
            files={"source": (path.name, f, "image/png")},
            timeout=60,
        )
    if resp.ok:
        url = resp.json().get("image", {}).get("url")
        if url:
            print(f"  Uploaded: {url}")
            return url
    print(f"  Upload failed: {resp.status_code} {resp.text[:100]}")
    return None


def add_to_notion(name, site_url, image_url):
    from dotenv import load_dotenv
    load_dotenv()
    token = os.getenv("NOTION_API_KEY") or os.getenv("NOTION_TOKEN")
    if not token:
        print(f"  No NOTION_API_KEY — skipping Notion for {name}")
        return

    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json",
        "Notion-Version": "2022-06-28",
    }
    blocks = [
        {"object": "block", "type": "heading_2", "heading_2": {
            "rich_text": [{"type": "text", "text": {"content": name}}]
        }},
        {"object": "block", "type": "paragraph", "paragraph": {
            "rich_text": [{"type": "text", "text": {"content": site_url, "link": {"url": site_url}}}]
        }},
        {"object": "block", "type": "image", "image": {
            "type": "external", "external": {"url": image_url}
        }},
        {"object": "block", "type": "divider", "divider": {}},
    ]
    resp = requests.patch(
        f"https://api.notion.com/v1/blocks/{NOTION_PAGE_ID}/children",
        headers=headers,
        json={"children": blocks},
        timeout=15,
    )
    if resp.ok:
        print(f"  Added to Notion: {name}")
    else:
        print(f"  Notion error: {resp.status_code} {resp.text[:200]}")


def main():
    retake = "--no-retake" not in sys.argv
    results = []

    if retake:
        with sync_playwright() as p:
            browser = p.chromium.launch(headless=False)
            context = browser.new_context(
                viewport={"width": VIEWPORT_W, "height": VIEWPORT_H},
                device_scale_factor=1,
            )
            page = context.new_page()
            for name, url in COMPETITORS:
                print(f"\n{name} ({url})")
                slug = name.lower().replace(" ", "-").replace("/", "")
                path = OUT_DIR / f"{slug}.png"
                take_screenshot(page, url, path)
            browser.close()

    for name, url in COMPETITORS:
        print(f"\n{name}")
        slug = name.lower().replace(" ", "-").replace("/", "")
        path = OUT_DIR / f"{slug}.png"
        if not path.exists():
            print(f"  No screenshot found at {path}, skipping")
            continue
        image_url = upload_image(path)
        if image_url:
            results.append((name, url, image_url))

    print(f"\nUploaded {len(results)}/{len(COMPETITORS)} screenshots")

    for name, site_url, image_url in results:
        print(f"\nAdding {name} to Notion...")
        add_to_notion(name, site_url, image_url)

    print("\nDone.")


if __name__ == "__main__":
    main()
