# Google Maps Business Scraper

A Python scraper for extracting business details from Google Maps, including:

- Name
- Website (cleaned direct URL)
- Phone
- Address
- Reviews
- Latitude & Longitude
- Business Email (skips free emails & placeholders)
- Maps URL

This scraper is designed to avoid duplicate entries and preserves order as seen on Google Maps.

---

## Features

- Cleans Google Maps redirect URLs to get the real business website.
- Filters out common free email domains (Gmail, Yahoo, Outlook, etc.).
- Skips duplicate businesses and duplicate websites.
- Saves data to CSV in UTF-8 format with proper quoting.
- Works asynchronously with `playwright` and `aiohttp`.

---

## Requirements

- Python 3.9+
- `playwright`
- `aiohttp`
- `pandas`

Install dependencies:

```bash
pip install playwright aiohttp pandas
playwright install




git clone https://github.com/yourusername/google-maps-scraper.git
cd google-maps-scraper
python scraper.py

