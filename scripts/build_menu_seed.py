import os, requests
from dotenv import load_dotenv

# load Supabase creds
load_dotenv(".env")
URL = os.environ["SUPABASE_URL"].rstrip("/")
KEY = os.environ["SUPABASE_SERVICE_ROLE_KEY"]

# output file
os.makedirs("data", exist_ok=True)
OUT = "data/menu_seed.txt"

print("Fetching websites from Supabase…")
res = requests.get(
    f"{URL}/rest/v1/restaurants",
    headers={"apikey": KEY, "Authorization": f"Bearer {KEY}"},
    params={"select": "website", "website": "not.is.null"},
    timeout=60,
)
res.raise_for_status()
rows = res.json()

webs = sorted({(row.get("website") or "").strip() for row in rows if row.get("website")})
with open(OUT, "w") as f:
    for w in webs:
        f.write(w + "\n")

print(f"Wrote {len(webs)} websites to {OUT}")
