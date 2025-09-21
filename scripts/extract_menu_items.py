import os, json, re, io, time, requests, tldextract
from urllib.parse import urlparse
from bs4 import BeautifulSoup
from pdfminer.high_level import extract_text as pdf_extract_text
from dotenv import load_dotenv

load_dotenv(".env")

CANDS = [
    "data/menu_candidates.jsonl",
    "data/menu_candidates_guessed.jsonl",
]
OUT = "data/menu_items.jsonl"

UA = {"User-Agent": "FareWare/0.1 (+contact: send.ishan@gmail.com)"}
PRICE_RX = re.compile(r"\$\s*\d{1,3}(?:\.\d{1,2})?|\b\d{1,3}\.\d{2}\b")
BAD_HOSTS = (
    "toasttab.com","square.site","doordash.com","ubereats.com",
    "grubhub.com","olo.com","chownow.com","opentable.com","resy.com",
)

def registrable(host:str)->str:
    ex=tldextract.extract(host or "")
    return ".".join([p for p in [ex.domain, ex.suffix] if p])

def host_of(u:str)->str:
    try: return urlparse(u).netloc
    except: return ""

def fetch(url):
    try:
        r=requests.get(url,headers=UA,timeout=25,allow_redirects=True)
        if r.status_code!=200: return None, None, None
        ct=r.headers.get("content-type","").lower()
        return r.content, r.text if "html" in ct else None, ct
    except: return None, None, None

def yield_items_from_html(html, url):
    soup=BeautifulSoup(html,"lxml")
    texts=[]
    for el in soup.select("h1,h2,h3,h4,h5,h6,li,p,span,div"):
        t=" ".join(el.get_text(" ",strip=True).split())
        if 3<=len(t)<=180:
            texts.append(t)
    for t in texts:
        prices=PRICE_RX.findall(t)
        if prices:
            parts=PRICE_RX.split(t, maxsplit=1)
            name=parts[0].strip(":-•–— ")
            p=float(prices[0].replace("$","")) if prices[0] else None
            if name and p:
                yield {"item_name":name, "item_desc":None, "price":p, "source_url":url}

def yield_items_from_pdf(bin_content, url):
    text = pdf_extract_text(io.BytesIO(bin_content)) or ""
    lines=[ln.strip() for ln in text.splitlines() if ln.strip()]
    for ln in lines:
        prices=PRICE_RX.findall(ln)
        if prices:
            name=PRICE_RX.split(ln, maxsplit=1)[0].strip(":-•–— ")
            try:
                p=float(prices[0].replace("$",""))
            except: continue
            if name and p:
                yield {"item_name":name, "item_desc":None, "price":p, "source_url":url}

def read_candidates():
    seen=set()
    for path in CANDS:
        if not os.path.exists(path): continue
        with open(path) as f:
            for line in f:
                try: j=json.loads(line)
                except: continue
                root=(j.get("root") or "").strip()
                url =(j.get("menu_url") or "").strip()
                if not url or url in seen: continue
                seen.add(url)
                yield root,url

def main():
    os.makedirs("data", exist_ok=True)
    out_f=open(OUT,"w")
    total=0; kept=0

    for root,url in read_candidates():
        total+=1
        h=host_of(url)
        if any(h.endswith(b) for b in BAD_HOSTS): 
            continue
        bin_content, html, ct = fetch(url)
        if not bin_content: 
            continue
        if html:
            for item in yield_items_from_html(html, url):
                out_f.write(json.dumps(item)+"\n"); kept+=1
        else:
            for item in yield_items_from_pdf(bin_content, url):
                out_f.write(json.dumps(item)+"\n"); kept+=1
        if kept and kept%50==0:
            print(f"wrote {kept} items…")
        time.sleep(0.05)

    out_f.close()
    print(f"Checked {total} pages | wrote {kept} items to {OUT}")

if __name__=="__main__":
    main()
