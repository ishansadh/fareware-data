from dotenv import load_dotenv
import os, requests, pathlib, json, sys

load_dotenv(".env")
URL = os.environ["SUPABASE_URL"].rstrip("/")
KEY = os.environ["SUPABASE_SERVICE_ROLE_KEY"]
HDR = {"apikey": KEY, "Authorization": f"Bearer {KEY}", "Prefer": "count=exact"}

print("Checking websites in DB …")
r = requests.get(f"{URL}/rest/v1/restaurants",
                 headers=HDR,
                 params={"select":"id","website":"not.is.null"},
                 timeout=30)
r.raise_for_status()
print("websites in DB (content-range):", r.headers.get("content-range","0/0"))

root = pathlib.Path(".")
seed = root / "data/menu_seed.txt"
cands = root / "data/menu_candidates.jsonl"

print("seed exists:", seed.exists(), "| lines:", sum(1 for _ in open(seed)) if seed.exists() else 0)
print("candidates exists:", cands.exists(),
      "| lines:", sum(1 for _ in open(cands)) if cands.exists() else 0)
if cands.exists():
    print("first few candidates:")
    with open(cands) as f:
        for i, line in enumerate(f):
            if i == 5: break
            print(line.strip())
