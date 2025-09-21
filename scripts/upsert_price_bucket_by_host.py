import os, json, requests, tldextract
from urllib.parse import urlparse
from dotenv import load_dotenv

load_dotenv(".env")
URL = os.environ["SUPABASE_URL"].rstrip("/")
KEY = os.environ["SUPABASE_SERVICE_ROLE_KEY"]
HDR = {"apikey": KEY, "Authorization": f"Bearer {KEY}", "Content-Type": "application/json"}

IN = "data/menu_prices.jsonl"

def registrable(host: str) -> str:
    ex = tldextract.extract(host or "")
    return ".".join([p for p in [ex.domain, ex.suffix] if p])

def host_of(url: str) -> str:
    try:
        return urlparse(url).netloc
    except Exception:
        return ""

def main():
    if not os.path.exists(IN):
        print("Missing data/menu_prices.jsonl — run import_menu_prices.py first.")
        return

    # 1) Build host -> bucket (majority vote if multiple)
    agg = {}
    with open(IN) as f:
        for line in f:
            j = json.loads(line)
            host = registrable(host_of(j.get("menu_url", "")))
            b = j.get("price_bucket")
            if not host or not b: 
                continue
            agg.setdefault(host, {}).setdefault(b, 0)
            agg[host][b] += 1

    host_bucket = {h: max(counts.items(), key=lambda kv: kv[1])[0] for h, counts in agg.items()}
    if not host_bucket:
        print("No buckets to upsert (menu_prices.jsonl empty or invalid).")
        return
    print(f"Hosts with buckets: {len(host_bucket)}")

    # 2) Fetch restaurant ids + website hosts
    ids_by_host = {}
    r = requests.get(
        f"{URL}/rest/v1/restaurants",
        headers=HDR,
        params={"select": "id,website", "website": "not.is.null"},
        timeout=90
    )
    r.raise_for_status()
    for row in r.json():
        host = registrable(host_of(row.get("website","")))
        if host:
            ids_by_host.setdefault(host, []).append(row["id"])

    # 3) PATCH rows by id (no insert possible)
    total = 0
    touched = 0
    for host, bucket in host_bucket.items():
        ids = ids_by_host.get(host, [])
        if not ids:
            continue
        for rid in ids:
            total += 1
            resp = requests.patch(
                f"{URL}/rest/v1/restaurants?id=eq.{rid}",
                headers=HDR,
                json={"price_bucket": bucket},
                timeout=30
            )
            if resp.status_code in (200, 204):
                touched += 1
            else:
                # noisy but useful for now
                print(f"PATCH failed id={rid} host={host} {resp.status_code} {resp.text[:160]}")
    print(f"Updated {touched}/{total} rows via PATCH")

if __name__ == "__main__":
    main()
