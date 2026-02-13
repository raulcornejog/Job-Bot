import os, json, yaml, hashlib
from datetime import datetime, timezone

import gspread
from google.oauth2.service_account import Credentials
from playwright.sync_api import sync_playwright

SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive",
]

NEW_TAB = "new_jobs"
SEEN_TAB = "seen_keys"
NEW_HEADERS = ["detected_at","company","title","location","url","source","key"]
SEEN_HEADERS = ["key","first_seen_at","company","title","location","url","source"]

def iso_now() -> str:
    return datetime.now(timezone.utc).isoformat()

def make_key(job: dict) -> str:
    base = f'{job["company"]}|{job.get("title","")}|{job.get("location","")}|{job.get("url","")}|{job.get("source","")}'
    return hashlib.sha256(base.encode("utf-8")).hexdigest()[:24]

def normalize(source: str, company: str, title: str, location: str, url: str) -> dict:
    return {
        "detected_at": iso_now(),
        "source": source,
        "company": company,
        "title": (title or "").strip(),
        "location": (location or "").strip(),
        "url": (url or "").strip(),
    }

class SheetsStore:
    def __init__(self, sa_key: dict, sheet_id: str):
        creds = Credentials.from_service_account_info(sa_key, scopes=SCOPES)
        gc = gspread.authorize(creds)
        self.sh = gc.open_by_key(sheet_id)
        self._ensure(NEW_TAB, NEW_HEADERS)
        self._ensure(SEEN_TAB, SEEN_HEADERS)

    def _ensure(self, tab: str, headers: list[str]):
        try:
            ws = self.sh.worksheet(tab)
        except gspread.WorksheetNotFound:
            ws = self.sh.add_worksheet(title=tab, rows=2000, cols=max(10, len(headers)))
        if not ws.row_values(1):
            ws.update("A1", [headers])

    def load_seen(self) -> set[str]:
        ws = self.sh.worksheet(SEEN_TAB)
        vals = ws.col_values(1)[1:]
        return set(v for v in vals if v)

    def replace_new(self, rows: list[dict]):
        ws = self.sh.worksheet(NEW_TAB)
        ws.clear()
        ws.update("A1", [NEW_HEADERS])
        if not rows:
            return
        values = [[r["detected_at"], r["company"], r["title"], r["location"], r["url"], r["source"], r["key"]] for r in rows]
        ws.update("A2", values)

    def append_seen(self, rows: list[dict]):
        if not rows:
            return
        ws = self.sh.worksheet(SEEN_TAB)
        values = [[r["key"], r["detected_at"], r["company"], r["title"], r["location"], r["url"], r["source"]] for r in rows]
        ws.append_rows(values, value_input_option="RAW")

def load_sources() -> list[dict]:
    with open("sources.yaml", "r", encoding="utf-8") as f:
        return yaml.safe_load(f)["sources"]

def scrape_generic(page, src) -> list[dict]:
    # Heurística: tomar anchors que parecen job detail (varía por sitio)
    page.goto(src["url"], wait_until="networkidle", timeout=60000)
    page.wait_for_timeout(1500)

    jobs = []
    anchors = page.locator("a").all()

    for a in anchors:
        href = a.get_attribute("href") or ""
        text = (a.inner_text() or "").strip()
        if not href or len(text) < 6:
            continue

        # Filtrado básico por dominio/rutas típicas (minimiza ruido)
        if src["name"] == "hellofresh":
            if "search-results" in href:
                continue
            if ("/job/" in href) or ("jobId" in href) or ("jobs/" in href):
                url = href if href.startswith("http") else f'https://careers.hellofresh.com{href}'
                jobs.append(normalize("hellofresh", src["company"], text, "", url))

        elif src["name"] == "uber":
            if ("jobs.uber.com" in href) or ("/careers/" in href and "uber.com" in href):
                url = href
                jobs.append(normalize("uber", src["company"], text, "Amsterdam, NL", url))

        elif src["name"] == "booking":
            if "/jobs/" in href:
                url = href if href.startswith("http") else f'https://jobs.booking.com{href}'
                jobs.append(normalize("booking", src["company"], text, "Amsterdam, NL", url))

    # uniq por URL
    uniq = {j["url"]: j for j in jobs if j.get("url")}
    return list(uniq.values())

def main():
    sa_key = json.loads(os.environ["GCP_SA_KEY"])
    sheet_id = os.environ["SHEET_ID"]

    store = SheetsStore(sa_key, sheet_id)
    seen = store.load_seen()

    sources = load_sources()
    new_rows = []
    newly_seen = []

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        context = browser.new_context(
            user_agent="Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121 Safari/537.36",
            locale="en-US",
        )
        page = context.new_page()

        for src in sources:
            jobs = scrape_generic(page, src)
            for j in jobs:
                j["key"] = make_key(j)
                if j["key"] not in seen:
                    new_rows.append(j)
                    newly_seen.append(j)
                    seen.add(j["key"])

        context.close()
        browser.close()

    store.replace_new(new_rows)
    store.append_seen(newly_seen)

if __name__ == "__main__":
    main()
