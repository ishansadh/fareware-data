import csv, os, requests
from dotenv import load_dotenv

load_dotenv(".env")
URL = os.environ["SUPABASE_URL"].rstrip("/")
KEY = os.environ["SUPABASE_SERVICE_ROLE_KEY"]

OUT = "data/missing_websites.csv"
os.makedirs("data", exist_ok=True)

res = requests.get(
    f"{URL}/rest/v1/restaurants",
    headers={"apikey": KEY, "Authorization": f"Bearer {KEY}"},
    params={"select":"id,name,addr,city,state,postcode,website",
            "website":"is.null","order":"name.asc"},
    timeout=60,
)
res.raise_for_status()
rows = res.json()

with open(OUT, "w", newline="") as f:
    w = csv.DictWriter(f, fieldnames=["id","name","addr","city","state","postcode","website"])
    w.writeheader()
    for r in rows:
        r["website"] = ""
        w.writerow(r)

print(f"Wrote {len(rows)} rows to {OUT}")
