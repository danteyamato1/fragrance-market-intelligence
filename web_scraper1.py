import requests
from bs4 import BeautifulSoup
import csv
import time

BASE_URL = "https://example.com"

headers = {
    "User-Agent": "Mozilla/5.0"
}

def get_soup(url):
    response = requests.get(url, headers=headers)
    return BeautifulSoup(response.text, "html.parser")


# 1. Get all product links from one listing page
def get_product_links(page_url):
    soup = get_soup(page_url)
    
    links = []
    
    for item in soup.select("a.product-link"):  # CHANGE THIS
        link = item.get("href")
        
        if link.startswith("/"):
            link = BASE_URL + link
        
        links.append(link)
    
    return links


# 2. Get data from product page
def get_product_data(product_url):
    soup = get_soup(product_url)
    
    try:
        name = soup.select_one("h1.product-title").text.strip()
    except:
        name = "N/A"
    
    try:
        price = soup.select_one(".price").text.strip()
    except:
        price = "N/A"
    
    # OPTIONAL: second page (like specs/reviews)
    try:
        extra_link = soup.select_one("a.more-info").get("href")
        
        if extra_link.startswith("/"):
            extra_link = BASE_URL + extra_link
        
        extra_info = get_extra_info(extra_link)
    except:
        extra_info = "N/A"
    
    return {
        "name": name,
        "price": price,
        "extra_info": extra_info,
        "url": product_url
    }


# 3. Extra page scraping
def get_extra_info(url):
    soup = get_soup(url)
    
    try:
        info = soup.select_one(".description").text.strip()
    except:
        info = "N/A"
    
    return info


# 4. Main scraper with pagination
def scrape_all_products():
    all_data = []
    
    total_pages = 5   # CHANGE THIS
    
    for page in range(1, total_pages + 1):
        print(f"\nScraping page {page}...")
        
        page_url = f"{BASE_URL}/products?page={page}"
        product_links = get_product_links(page_url)
        
        print(f"Found {len(product_links)} products")
        
        for link in product_links:
            print(f"Scraping product: {link}")
            
            data = get_product_data(link)
            all_data.append(data)
            
            time.sleep(1)  # avoid blocking
    
    return all_data


# 5. Save data
def save_to_csv(data):
    with open("products.csv", "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=data[0].keys())
        writer.writeheader()
        writer.writerows(data)


# Run
if __name__ == "__main__":
    data = scrape_all_products()
    save_to_csv(data)
    print("\n Done! Saved to products.csv")