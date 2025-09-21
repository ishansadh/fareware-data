import os, json, re, time, requests, io, tldextract
from urllib.parse import urlparse
from dotenv import load_dotenv
from pdfminer.high_level import extract_text as pdf_extract_text

load_dotenv(".env")
URL = os.environ["SUPABASE_URL"].rstrip("/")
KEY = os.environ["SUPABASE_SERVICE_ROLE_KEY"]

CANDS_A = "data/menu_candidates.jsonl"          # crawler (optional)
CANDS_B = "data/menu_candidates_guessed.jsonl"  # guesser (we have this)
OUT     = "data/menu_prices.jsonl"

# Known 3rd-party ordering hosts we want to SKIP (JS heavy / no inline prices)
THIRDPARTY = (
    "toasttab.com", "square.site", "tryotter.com", "clover.com",
    "ubereats.com", "doordash.com", "grubhub.com", "olo.com",
    "chownow.com", "opentable.com", "resy.com", "ezcater.com"
)

PRICE_RX = re.compile(r"\$\s*\d{1,3}(?:\.\d{1,2})?")
ALT_RX   = re.compile(r"(?<!\d)(\d{1,3}\.\d{2})(?!\d)")  # 12.99 without $

UA = {"User-Agent": "FareWare/0.1 (+contact: send.ishan@gmail.com)"}

def bucket(m):
    if m < 10:  return "$"
    if m < 20:  return "$$"
    if m < 35:  return "$$$"
    return "$$$$"

def fetch_html(url):
    try:
        r = requests.get(url, headers=UA, timeout=25)
        if r.status_code == 200 and "text/html" in r.headers.get("content-type","").lower():
            return r.text
    except Exception:
        pass
    return ""

def fetch_pdf_text(url):
    try:
        r = requests.get(url, headers=UA, timeout=30)
        if r.status_code == 200 and ("pdf" in r.headers.get("content-type","").lower() or url.lower().endswith(".pdf")):
            return pdf_extract_text(io.BytesIO(r.content))
    except Exception:
        pass
    return ""

def registrable(host):
    ext = tldextract.extract(host)
    return ".".join([p for p in [ext.domain, ext.suffix] if p])

def same_org(a, b):
    return registrable(a) == registrable(b)

def read_candidates():
    seen = set()
    for path in (CANDS_A, CANDS_B):
        if not os.path.exists(path): continue
        with open(path) as f:
            for line in f:
                try:
                    j = json.loads(line)
                    root = (j.get("root") or "").strip()
                    url  = (j.get("menu_url") or "").strip()
                    if not url or url in seen: 
                        continue
                    seen.add(url)
                    yield root, url
                except Exception:
                    continue

def to_floats(found):
    out=[]
    for p in found:
        try: out.append(float(p.replace("$","").strip()))
        except: pass
    return out

def main():
    os.makedirs("data", exist_ok=True)
    total = 0
    derived = 0
    out = []

    for root, url in read_candidates():
        total += 1
        # 1) skip obvious third-party ordering systems
        host = urlparse(url).netloc
        if any(host.endswith(tp) for tp in THIRDPARTY):
            continue
        # 2) prefer same-organization domain only
        if root:
            rhost = urlparse(root).netloc
            if rhost and (not same_org(host, rhost)):
                continue

        # 3) fetch & parse
        is_pdf = url.lower().endswith(".pdf")
        text = fetch_pdf_text(url) if is_pdf else fetch_html(url)
        if not text:
            continue

        found = PRICE_RX.findall(text) or ALT_RX.findall(text)
        nums  = to_floats(found)
        # basic guardrails against false positives (e.g. 2024, 773 phone)
        nums  = [n for n in nums if 1 <= n <= 200]
        if not nums:
            continue

        nums.sort()
        med = nums[len(nums)//2]
        out.append({"menu_url": url, "median_price": med, "price_bucket": bucket(med)})
        derived += 1
        if derived % 25 == 0:
            print(f"derived {derived} …")
        time.sleep(0.1)

    with open(OUT, "w") as f:
        for rec in out:
            f.write(json.dumps(rec) + "\n")

    print(f"Candidates checked: {total} | Derived price pages: {derived}")
    if out:
        print("Sample:", out[:3])
    else:
        print("No prices found — next step: fill more first-party websites (CSV enrichment) or enable headless for JS pages.")

if __name__ == "__main__":
    main()
