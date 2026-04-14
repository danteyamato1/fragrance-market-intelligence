"""
debug_votes.py — Run this in your project folder to diagnose why votes are None.
It fetches one page and dumps exactly what's in the tw-rating-card HTML.

Usage:  python debug_votes.py
"""
import cloudscraper
from bs4 import BeautifulSoup, Tag

URL = "https://www.fragrantica.com/perfume/Dior/Fahrenheit-228.html"

scraper = cloudscraper.create_scraper(
    browser={"browser": "chrome", "platform": "windows", "desktop": True}
)

print(f"Fetching {URL} ...")
resp = scraper.get(URL, timeout=20)
print(f"Status: {resp.status_code}  |  HTML length: {len(resp.text):,} chars")

soup = BeautifulSoup(resp.text, "html.parser")


cards = soup.find_all(class_="tw-rating-card")
print(f"\n[1] tw-rating-card elements found: {len(cards)}")

if not cards:
    print("\n  !! CARDS NOT FOUND — the page is probably rendering them client-side (JS)")
    print("     Searching for any mention of 'love' / 'tabular-nums' in the page...")
    text = resp.text
    print(f"  'tabular-nums' in HTML: {'tabular-nums' in text}")
    print(f"  'tw-rating-card' in HTML: {'tw-rating-card' in text}")
    print(f"  '10.5k' in HTML: {'10.5k' in text}")

    idx = text.find("tw-rating-card")
    if idx >= 0:
        print(f"\n  Snippet around 'tw-rating-card':\n{text[idx:idx+500]}")
else:
    for ci, card in enumerate(cards):
        lbl = card.find(class_="tw-rating-card-label")
        label_text = lbl.get_text(strip=True) if lbl else "???"
        blocks = card.select("div.flex-col.items-center")
        print(f"\n[2] Card {ci}: '{label_text}' — flex-col.items-center blocks: {len(blocks)}")

        for bi, block in enumerate(blocks):
            label_span = None
            for child in block.children:
                if isinstance(child, Tag) and child.name == "span":
                    label_span = child.get_text(strip=True)
                    break
            count_span = block.find("span", class_="tabular-nums")
            count = count_span.get_text(strip=True) if count_span else "MISSING"
            print(f"   block[{bi}]: first-child-span={label_span!r:12}  tabular-nums={count!r}")

        if not blocks:
            print("   !! No flex-col.items-center blocks — dumping card HTML:")
            print(card.prettify()[:1500])
