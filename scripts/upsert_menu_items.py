import os, json, requests, tldextract
from urllib.parse import urlparse
from dotenv import load_dotenv

load_dotenv(".env")
URL = os.environ["SUPABASE_URL"].rstrip("/")
KEY = os.environ["SUPABASE_SERVICE_ROLE_KEY"]
HDR = {
  "apikey": KEY,
  "Authorization": f"Bearer {KEY}",
  "Content-Type": "application/json",
  "Prefer": "resolution=merge-duplicates"
}

IN = "data/menu_items.jsonl"
BATCH = 500

def registrable(host:str)->str:
    ex=tldextract.extract(host or "")
    return ".".join([p for p in [ex.domain, ex.suffix] if p])

def host_of(u:str)->str:
    try: return urlparse(u).netloc
    except: return ""

def main():
    if not os.path.exists(IN):
        print("Missing data/menu_items.jsonl. Run extract_menu_items.py first.")
        return

    # host -> [rows]
    by_host={}
    with open(IN) as f:
        for line in f:
            j=json.loads(line)
            host = registrable(host_of(j["source_url"]))
            by_host.setdefault(host,[]).append(j)

    # fetch restaurants with website to map host -> ids
    ids_by_host={}
    r=requests.get(f"{URL}/rest/v1/restaurants",
                   headers=HDR,
                   params={"select":"id,website","website":"not.is.null"},
                   timeout=120)
    r.raise_for_status()
    for row in r.json():
        h=registrable(host_of(row.get("website","")))
        if h: ids_by_host.setdefault(h,[]).append(row["id"])

    total=0; inserted=0
    for host, items in by_host.items():
        ids=ids_by_host.get(host,[])
        if not ids: 
            continue
        # assign each item to all matching restaurants on that host (usually 1)
        rows=[]
        for rid in ids:
            for it in items:
                rows.append({
                    "restaurant_id": rid,
                    "item_name": it.get("item_name"),
                    "item_desc": it.get("item_desc"),
                    "price": it.get("price"),
                    "currency": "USD",
                    "category": None,
                    "source_url": it.get("source_url"),
                    "calories_kcal": it.get("calories_kcal"),
                    "calories_text": it.get("calories_text"),
                    
                })
        # send in batches (uses unique constraint to drop dupes)
        for i in range(0, len(rows), BATCH):
            chunk = rows[i:i+BATCH]
            res=requests.post(
                f"{URL}/rest/v1/menu_items_v2?on_conflict=restaurant_id,item_name,price,source_url",
                headers=HDR,
                json=chunk,
                timeout=120
            )
            if res.status_code not in (200,201,204):
                print("Insert error", host, res.status_code, res.text[:200])
                continue
            inserted += len(chunk)
        total += len(rows)
    print(f"Prepared {total} rows | attempted insert {inserted} (duplicates are ignored by on_conflict)")

if __name__=="__main__":
    main()
