import csv, os, requests
from urllib.parse import urlparse
from dotenv import load_dotenv

load_dotenv(".env")
URL = os.environ["SUPABASE_URL"].rstrip("/")
KEY = os.environ["SUPABASE_SERVICE_ROLE_KEY"]

INFILE = "data/missing_websites.csv"
REST = f"{URL}/rest/v1/restaurants"

def clean_url(u:str)->str:
    if not u: return ""
    u=u.strip()
    if not u: return ""
    if not u.startswith(("http://","https://")): u="https://"+u
    p=urlparse(u)
    return f"{p.scheme}://{p.netloc}{p.path}".rstrip("/")

updates=[]
with open(INFILE, newline="") as f:
    r=csv.DictReader(f)
    for row in r:
        w=clean_url(row.get("website",""))
        if w: updates.append({"id":row["id"], "website":w})

if not updates:
    print("No website updates found.")
    raise SystemExit(0)

res=requests.post(
    REST+"?on_conflict=id",
    headers={"apikey":KEY,"Authorization":f"Bearer {KEY}","Content-Type":"application/json",
             "Prefer":"resolution=merge-duplicates"},
    json=updates, timeout=60
)
print(res.status_code, res.text[:300])
res.raise_for_status()
print(f"Updated {len(updates)} websites.")
