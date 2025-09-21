import os, csv, time, requests
from dotenv import load_dotenv

load_dotenv(".env")
URL = os.environ["SUPABASE_URL"].rstrip("/")
KEY = os.environ["SUPABASE_SERVICE_ROLE_KEY"]

INPUT = "data/public_list.csv"   # <- put your CSV here
UA = "FareWare/0.1 (contact: send.ishan@gmail.com)"
NOMINATIM = "https://nominatim.openstreetmap.org/search"

def geocode(addr):
    params = {"q": addr, "format":"json", "limit":1}
    r = requests.get(NOMINATIM, params=params, headers={"User-Agent":UA}, timeout=30)
    r.raise_for_status()
    j = r.json()
    if not j: return None, None
    return float(j[0]["lat"]), float(j[0]["lon"])

def clean(s): return (s or "").strip() or None

def main():
    rows=[]
    with open(INPUT, newline="") as f:
        r=csv.DictReader(f)
        for row in r:
            name=clean(row.get("name"))
            address=clean(row.get("address"))
            city=clean(row.get("city"))
            state=clean(row.get("state"))
            postcode=clean(row.get("postcode"))
            phone=clean(row.get("phone"))
            website=clean(row.get("website"))
            if not (name and (address or city)):
                continue
            full = " ".join([p for p in [address, city, state, postcode] if p])
            lat, lon = geocode(full)  # polite throttle
            time.sleep(1.1)
            rows.append({
                "name": name, "addr": address, "city": city, "state": state,
                "postcode": postcode, "phone": phone, "website": website,
                "lat": lat, "lon": lon,
                "source": "license", "source_ref": "public_list",
                "status": "active"
            })

    print(f"Prepared {len(rows)} rows; upserting...")
    res = requests.post(
        f"{URL}/rest/v1/restaurants?on_conflict=name_norm,lat_round,lon_round",
        headers={"apikey":KEY,"Authorization":f"Bearer {KEY}","Content-Type":"application/json",
                 "Prefer":"resolution=merge-duplicates"},
        json=rows, timeout=120
    )
    print(res.status_code, res.text[:400])
    res.raise_for_status()
    print("Done.")

if __name__ == "__main__":
    main()
