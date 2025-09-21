import os, requests, json, time
from urllib.parse import urljoin, urlparse
from dotenv import load_dotenv

load_dotenv(".env")
URL = os.environ["SUPABASE_URL"].rstrip("/")
KEY = os.environ["SUPABASE_SERVICE_ROLE_KEY"]

# Common menu paths to probe
PATHS = [
    "/menu", "/menus", "/our-menu", "/food", "/dine", "/dining",
    "/lunch", "/dinner", "/breakfast", "/brunch", "/happy-hour",
    "/catering", "/order", "/order-online", "/takeout", "/to-go", "/pdfs/menu.pdf"
]

OUT = "data/menu_candidates_guessed.jsonl"
UA = {"User-Agent": "FareWare/0.1 (+contact: send.ishan@gmail.com)"}

def normalize_site(u: str) -> str:
    u = (u or "").strip()
    if not u: return ""
    if not u.startswith(("http://","https://")): u = "https://" + u
    # strip query/fragment
    p = urlparse(u)
    return f"{p.scheme}://{p.netloc}"

def main():
    os.makedirs("data", exist_ok=True)

    # Pull all websites from DB
    r = requests.get(
        f"{URL}/rest/v1/restaurants",
        headers={"apikey": KEY, "Authorization": f"Bearer {KEY}"},
        params={"select": "website", "website": "not.is.null"},
        timeout=60
    )
    r.raise_for_status()
    sites = sorted({normalize_site(row.get("website")) for row in r.json() if row.get("website")})
    if not sites:
        print("No websites found. Consider running website enrichment first.")
        return

    print(f"Testing {len(sites)} sites × {len(PATHS)} paths …")
    total = 0
    hits = 0
    with open(OUT, "w") as f:
        for base in sites:
            if not base: continue
            for path in PATHS:
                total += 1
                url = urljoin(base, path)
                try:
                    # HEAD first (cheap), fallback to GET if server refuses HEAD
                    resp = requests.head(url, headers=UA, timeout=10, allow_redirects=True)
                    if resp.status_code >= 400 or ('content-type' not in resp.headers):
                        resp = requests.get(url, headers=UA, timeout=15, allow_redirects=True)
                    if resp.status_code < 400:
                        ct = resp.headers.get("content-type","").lower()
                        if ("text/html" in ct) or ct.endswith("/pdf") or ("pdf" in ct):
                            f.write(json.dumps({"root": base, "menu_url": url, "content_type": ct}) + "\n")
                            hits += 1
                except Exception:
                    pass
                time.sleep(0.1)  # gentle pacing
    print(f"Probed {total} URLs → found {hits} candidate menu pages")
    print(f"Wrote: {OUT}")

if __name__ == "__main__":
    main()
