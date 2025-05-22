import pandas as pd
import requests
import base64
import re
import time
import pytz
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed

# --- Settings ---
zyte_api_key = "91ae5e88754f4ddb843724420c64eb9b"
headers = {"Content-Type": "application/json"}
MAX_WORKERS = 10  # Adjust depending on your machine/network

def get_netherlands_time():
    tz = pytz.timezone("Europe/Amsterdam")
    return datetime.now(tz).strftime("%-m/%-d/%Y, %-I:%M %p")

def scrape_disposablediscounter(mpn):
    url = f"https://www.disposablediscounter.nl/catalogsearch/result/?q={mpn}"
    try:
        html = requests.get(url, timeout=10).text
        nbsp = '\u00a0'
        match = re.search(
            rf'<span[^>]*class="[^"]*price-wrapper[^"]*price-including-tax[^"]*"[^>]*>\s*<span[^>]*class="price">€(?:\s|{nbsp}|&nbsp;)*([0-9.,]+)</span>',
            html, re.IGNORECASE
        )
        return f"€ {match.group(1).strip()}" if match else "Not Found"
    except:
        return "Not Found"

def scrape_discountoffice(gtin):
    payload = {"url": f"https://discountoffice.nl/?q={gtin}", "httpResponseBody": True}
    try:
        r = requests.post(
            "https://api.zyte.com/v1/extract",
            json=payload,
            headers=headers,
            auth=(zyte_api_key, "")
        )
        encoded = r.json().get("httpResponseBody")
        if not encoded:
            return "Not Found"
        html = base64.b64decode(encoded).decode("utf-8")
        if "<title>Verification</title>" in html:
            return "Blocked"
        nbsp = '\u00a0'
        match = re.search(
            rf'<span class="label label-price">[^€]*€(?:\s|{nbsp}|&nbsp;)*([0-9.,]+)[^<]*</span>',
            html, re.IGNORECASE
        )
        return f"€ {match.group(1).strip()}" if match else "Not Found"
    except:
        return "Not Found"

def process_row(index, row, sheet_name):
    mpn = str(row["MPN"])
    gtin = str(row["Gtin"])
    price_dd = scrape_disposablediscounter(mpn)
    price_do = scrape_discountoffice(gtin)
    update_time = get_netherlands_time()
    print(f"{sheet_name}, {mpn}, {gtin}, {price_do}, {price_dd}, {update_time}")
    return index, price_do, price_dd, update_time

def process_csv(file_path):
    df = pd.read_csv(file_path)
    sheet_name = file_path
    results = []

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = [
            executor.submit(process_row, idx, row, sheet_name)
            for idx, row in df.iterrows()
        ]
        for future in as_completed(futures):
            index, price_do, price_dd, update_time = future.result()
            df.at[index, "Price DO"] = price_do
            df.at[index, "Price DD"] = price_dd
            df.at[index, "UpdateUTC"] = update_time

    df.to_csv(file_path, index=False)

# --- Run both files ---
if __name__ == "__main__":
    process_csv("simple_product_pricescrape.csv")
    process_csv("child_product_pricescrape.csv")
