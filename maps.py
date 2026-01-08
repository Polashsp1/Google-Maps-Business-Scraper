import asyncio
import re
import pandas as pd
import aiohttp
import os
import csv
from urllib.parse import urlparse, parse_qs, unquote, urlunparse
from playwright.async_api import async_playwright

# --- CONFIGURATION ---
TARGET_URL = "https://www.google.com/maps/search/spa+in+Russia/@57.4464695,29.1888173,5z/data=!3m1!4b1?entry=ttu&g_ep=EgoyMDI2MDEwNC4wIKXMDSoKLDEwMDc5MjA2N0gBUAM%3D=en"
MAX_RESULTS = 1000
CSV_FILE = "spa_in_russia.csv"
EMAIL_REGEX = r'[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}'

# ---------- CSV CLEAN FIX ----------
def clean_csv_value(value):
    if not value:
        return ""
    return (
        str(value)
        .replace("\n", " ")
        .replace("\r", " ")
        .replace("\t", " ")
        .strip()
    )

# ---------- BUSINESS EMAIL FILTER ----------
FREE_DOMAINS = ["gmail.com", "yahoo.com", "hotmail.com", "outlook.com", "aol.com", "icloud.com", "live.com"]
PLACEHOLDERS = ["user@domain.com", "example@example.com", "test@test.com"]

def is_business_email(email):
    email = email.lower().strip()
    domain = email.split("@")[-1]
    if email in PLACEHOLDERS:
        return False
    return domain not in FREE_DOMAINS

# ---------- CLEAN GOOGLE URL ----------
def clean_google_url(url):
    """
    Converts Google Maps URL or redirect URL to direct website URL.
    Handles https://www.google.com/url?... type URLs.
    """
    if not url:
        return ""
    
    url = url.strip()
    
    # If Google redirect URL, extract actual target
    if "google.com/url" in url:
        try:
            parsed = urlparse(url)
            qs = parse_qs(parsed.query)
            target = qs.get("q", [""])[0]
            url = unquote(target)
        except:
            pass

    # Remove trailing slash
    url = url.rstrip("/")
    
    return url

# ---------- NORMALIZE WEBSITE FOR DUPLICATE CHECK ----------
def normalize_website(url):
    """
    Normalize website for duplicate detection:
    - lowercase
    - remove http/https
    - remove trailing slash
    """
    if not url:
        return ""
    url = url.lower().strip()
    if url.startswith("http://"):
        url = url[7:]
    elif url.startswith("https://"):
        url = url[8:]
    return url.rstrip("/")

# ---------- FETCH EMAILS FROM WEBSITE ----------
async def fetch_emails(session, url):
    if not url or not url.startswith("http"):
        return ""
    
    try:
        async with session.get(url, timeout=10, headers={"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/143.0.0.0 Safari/537.36"}) as response:
            text = await response.text(errors="ignore")
            emails = re.findall(EMAIL_REGEX, text)
            seen_emails = set()
            for email in emails:
                email = email.lower().strip()
                if email in seen_emails:
                    continue
                seen_emails.add(email)
                if is_business_email(email):
                    return email
            return ""
    except:
        return ""

# ---------- EXTRACT LATITUDE & LONGITUDE ----------
def extract_lat_lng(url):
    match = re.search(r'!3d([-+]?\d+\.\d+)!4d([-+]?\d+\.\d+)', url)
    if match:
        return match.group(1), match.group(2)
    return None, None

# ---------- MAIN SCRAPER ----------
async def run_scraper():
    seen_names = set()
    seen_contacts = set()
    seen_websites = set()
    results_count = 0

    # create CSV header once
    if not os.path.isfile(CSV_FILE):
        header_df = pd.DataFrame(columns=[
            "name", "website", "phone", "address",
            "reviews", "latitude", "longitude",
            "email", "maps_url"
        ])
        header_df.to_csv(CSV_FILE, index=False, encoding="utf-8-sig")

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=False)
        context = await browser.new_context(locale="en-GB")
        page = await context.new_page()

        print("[*] Navigating to Google Maps...")
        await page.goto(TARGET_URL, wait_until="domcontentloaded", timeout=60000)

        try:
            await page.wait_for_selector('div[role="article"]', timeout=20000)
        except:
            pass

        async with aiohttp.ClientSession() as session:
            while results_count < MAX_RESULTS:
                listings = await page.query_selector_all('div[role="article"]')
                new_batch_found = False

                for listing in listings:
                    try:
                        name_el = await listing.query_selector('div.qBF1Pd')
                        if not name_el:
                            continue

                        name = await name_el.inner_text()
                        if name in seen_names:
                            continue

                        new_batch_found = True
                        await listing.scroll_into_view_if_needed()
                        await asyncio.sleep(1)
                        await listing.click()
                        await page.wait_for_timeout(2000)

                        web_el = await page.query_selector('a[data-item-id="authority"]')
                        raw_website = await web_el.get_attribute('href') if web_el else ""
                        website = clean_google_url(raw_website)
                        norm_website = normalize_website(website)

                        # Skip duplicate websites
                        if norm_website in seen_websites or not website:
                            continue
                        seen_websites.add(norm_website)

                        phone_el = await page.query_selector('button[data-item-id^="phone"]')
                        phone = await phone_el.inner_text() if phone_el else "N/A"

                        addr_el = await page.query_selector('button[data-item-id="address"]')
                        address = await addr_el.inner_text() if addr_el else "N/A"

                        review_el = await page.query_selector('div.F7nice')
                        reviews = await review_el.inner_text() if review_el else "0"

                        lat, lng = extract_lat_lng(page.url)
                        email = await fetch_emails(session, website) if website else ""

                        # ---------- CHECK DUPLICATES ----------
                        contact_key = f"{name}|{website}|{phone}|{address}|{lat}|{lng}"
                        if contact_key in seen_contacts:
                            continue
                        seen_contacts.add(contact_key)

                        # ---------- SAVE TO CSV ----------
                        new_row = pd.DataFrame([{
                            "name": clean_csv_value(name),
                            "website": clean_csv_value(website),
                            "phone": clean_csv_value(phone),
                            "address": clean_csv_value(address),
                            "reviews": clean_csv_value(reviews),
                            "latitude": lat,
                            "longitude": lng,
                            "email": clean_csv_value(email),
                            "maps_url": clean_csv_value(page.url)
                        }])

                        new_row.to_csv(
                            CSV_FILE,
                            mode='a',
                            header=False,
                            index=False,
                            encoding="utf-8-sig",
                            lineterminator="\n",
                            quoting=csv.QUOTE_ALL
                        )

                        seen_names.add(name)
                        results_count += 1
                        print(f"[{results_count}] Saved: {name}")

                        if results_count >= MAX_RESULTS:
                            break

                    except Exception as e:
                        print(f"Error processing listing: {e}")
                        continue

                # ---------- SCROLL FOR MORE RESULTS ----------
                if not new_batch_found:
                    await page.mouse.move(200, 400)
                    await page.mouse.wheel(0, 3000)
                    await asyncio.sleep(4)

        await browser.close()


if __name__ == "__main__":
    asyncio.run(run_scraper())
