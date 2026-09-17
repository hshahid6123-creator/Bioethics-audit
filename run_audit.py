#!/usr/bin/env python3
"""
run_audit.py - Headless Bioethics Audit Pipeline Master CLI
Fully Integrated with 16-Column Schema, 2017+ Year Filter, 4-Point AI Optimization,
Embedded Multi-Key Configuration, Phase 3 (Download), Phase 4 (Markdown + OCR Fallback),
SQLite Persistence, and 30% Error Circuit Breaker.
"""
import os
import sys
import io
import json
import time
import shutil
import random
import argparse
import logging
from pathlib import Path
from typing import Optional, List, Dict, Any

# Ensure stdout handles UTF-8 safely on Windows with immediate flushing
if sys.platform == "win32":
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace", line_buffering=True)
    sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding="utf-8", errors="replace", line_buffering=True)

# Required document and OCR dependencies
import fitz  # PyMuPDF
import pymupdf4llm
import pytesseract
from PIL import Image
import requests
from pydantic import BaseModel, Field

# API Clients
from google import genai
from openai import OpenAI

# Project Module Imports
sys.path.insert(0, str(Path(__file__).resolve().parent))
from modules.archive.crawler import JournalCrawler
from modules.storage.database import init_db, upsert_audit_record

# ---------------------------------------------------------
# DIRECTORY DEFINITIONS
# ---------------------------------------------------------
PDF_DIR = Path("downloads/pdfs/articles")
MD_DIR = Path("markdown_texts")
LOG_DIR = Path("logs")
PDF_DIR.mkdir(parents=True, exist_ok=True)
MD_DIR.mkdir(parents=True, exist_ok=True)
LOG_DIR.mkdir(parents=True, exist_ok=True)

# ---------------------------------------------------------
# PHASE 0: CONFIG & MULTI-KEY ROTATOR
# ---------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger("MasterPipelineCLI")

# Embedded API Keys to bypass manual .env setup in Antigravity
HARDCODED_KEYS = {
    "GEMINI": [
        "AQ.Ab8RN6JmrEGZ_zTAZng4tQNwt_3W-n7UsXLihfMkFdD0rrZLCw",
        "AQ.Ab8RN6JVRWbbROy2xJIja2_7bn0GD51XmriALX66FVmmoiYixA"
    ],
    "CEREBRAS": [
        "csk-mm4mf89c3y2mejfkd2p8k8cfyxynjd8rvedr22h3jjrf6cv5",
        "csk-xee4j2cvthy465j6mv443p8hh2df94nwh4k9h66kph8x3658",
        "csk-hj64w9xktxprhkwfnhpkvprdv96n9md3j6hchmfwyr8pmw4v"
    ],
    "GROQ": [
        "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",
        "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",
        "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"
    ]
}

# Telemetry Tracker
class PipelineTelemetry:
    def __init__(self):
        self.start_time = time.time()
        self.end_time = None
        self.api_calls = {"GEMINI": 0, "CEREBRAS": 0, "GROQ": 0}
        self.key_usage = {"GEMINI": {}, "CEREBRAS": {}, "GROQ": {}}
        self.tier_counts = {}
        self.article_latencies = []
        self.fallbacks_triggered = 0

telemetry = PipelineTelemetry()

class APIKeyPool:
    def __init__(self, provider_name: str):
        self.provider_name = provider_name.upper()
        self.keys = HARDCODED_KEYS.get(self.provider_name, []).copy()
        
        i = 1
        while True:
            key = os.getenv(f"{self.provider_name}_API_KEY_{i}")
            if not key:
                break
            if key not in self.keys:
                self.keys.append(key)
            i += 1
            
        if not self.keys:
            logger.warning(f"No keys configured for {self.provider_name}")
        self.current_index = 0

    def get_next_key(self) -> str:
        key = self.keys[self.current_index]
        key_masked = key[:6] + "..." + key[-4:]
        telemetry.key_usage[self.provider_name][key_masked] = telemetry.key_usage[self.provider_name].get(key_masked, 0) + 1
        self.current_index = (self.current_index + 1) % len(self.keys)
        return key

class UnifiedAPIExecutor:
    def __init__(self):
        self.pools = {
            "GEMINI": APIKeyPool("GEMINI"),
            "CEREBRAS": APIKeyPool("CEREBRAS"),
            "GROQ": APIKeyPool("GROQ")
        }
        self.delays = {"GEMINI": 1.5, "CEREBRAS": 1.0, "GROQ": 0.8}

    def execute_with_retry(self, provider: str, payload_func, max_retries: int = 4):
        provider = provider.upper()
        pool = self.pools[provider]
        
        attempts = 0
        total_keys = max(1, len(pool.keys))
        max_attempts = max(max_retries, total_keys * 2)

        while attempts < max_attempts:
            current_key = pool.get_next_key()
            try:
                time.sleep(self.delays[provider])
                telemetry.api_calls[provider] = telemetry.api_calls.get(provider, 0) + 1
                if provider == "GEMINI":
                    client = genai.Client(api_key=current_key)
                    return payload_func(client)
                else:
                    base_url = "https://api.cerebras.ai/v1" if provider == "CEREBRAS" else "https://api.groq.com/openai/v1"
                    client = OpenAI(base_url=base_url, api_key=current_key)
                    return payload_func(client)
            except Exception as e:
                error_msg = str(e).lower()
                is_rate_limit = any(code in error_msg for code in ["429", "rate_limit", "resource_exhausted", "too many requests"])
                is_billing_or_not_found = any(code in error_msg for code in ["402", "payment_required", "not_found_error", "model_not_found"])
                
                attempts += 1
                if is_billing_or_not_found and provider == "CEREBRAS":
                    logger.warning(f"[CEREBRAS] Quota/Limitation ({e}). Activating automatic fallback to GROQ...")
                    telemetry.fallbacks_triggered += 1
                    def groq_fallback(groq_client):
                        return payload_func(groq_client)
                    return self.execute_with_retry("GROQ", groq_fallback, max_retries=max_retries)

                if is_rate_limit:
                    logger.warning(f"[{provider}] Rate limit on attempt {attempts}. Rotating key...")
                    if attempts % total_keys == 0:
                        sleep_time = (2 ** (attempts // total_keys)) + random.uniform(1, 3)
                        logger.info(f"[{provider}] All keys exhausted. Backoff: {sleep_time:.1f}s...")
                        time.sleep(sleep_time)
                else:
                    logger.error(f"[{provider}] Error: {str(e)[:120]}")
                    if attempts >= max_attempts:
                        raise e
        raise RuntimeError(f"[{provider}] Maximum retry attempts exhausted.")

executor = UnifiedAPIExecutor()

# ---------------------------------------------------------
# MANDATORY EXACT 16-COLUMN SCHEMA
# ---------------------------------------------------------
class BioethicsRecord(BaseModel):
    article_id: str
    article_title: str
    authors: str
    journal_name: str
    year: int
    study_design: str
    ethical_approval_status: str
    ethics_committee_name: str
    ethics_ref_number: str
    verbatim_ethics_quote: str
    ethics_page: str
    informed_consent_status: str
    informed_consent_evidence: str
    informed_consent_page: str
    ai_tier_classification: str
    pdf_url: str

# ---------------------------------------------------------
# OPTIMIZATION POINT 4: DETERMINISTIC CONFLICT ARBITRATION
# ---------------------------------------------------------
def determine_final_tier(evidence: dict) -> str:
    """Rules engine overriding AI. AI cannot bypass these absolute policy rules."""
    design = evidence.get("study_design", "").lower()
    if any(x in design for x in ["review", "editorial", "commentary", "meta-analysis", "case_report", "systematic review"]):
        return "Exempt: Non-Empirical/Case"

    committee = str(evidence.get("ethics_committee_name", "")).strip()
    has_committee = committee not in ["Not Reported", "None", "not_reported", "N/A", ""] and len(committee) > 2
    
    ref_num = str(evidence.get("ethics_ref_number", "")).strip()
    has_ref_num = ref_num not in ["Not Reported", "None", "not_reported", "N/A", ""] and len(ref_num) > 1
    
    consent = str(evidence.get("informed_consent_status", "")).strip().lower()
    has_consent = consent not in ["not_reported", "no_consent", "none", ""] and "not" not in consent

    if has_committee and has_ref_num and has_consent:
        return "Gold"
    elif has_committee and (has_consent or has_ref_num):
        return "Tier 1: Minor Omission"
    elif has_committee or has_consent:
        return "Tier 2: Major Omission"
    else:
        return "Tier 3: Silent Omission"

# ---------------------------------------------------------
# A. IMPORTS & OCR FALLBACK ENGINE
# ---------------------------------------------------------
def ocr_fallback_pdf(pdf_path: Path) -> str:
    """Renders PDF pages to images and extracts text using Tesseract OCR."""
    logger.info(f"Triggering OCR fallback for {pdf_path.name}...")
    doc = fitz.open(str(pdf_path))
    ocr_pages = []
    
    for page_num in range(len(doc)):
        page = doc[page_num]
        # Render page at 2x resolution (144 DPI) for OCR clarity
        pix = page.get_pixmap(dpi=144)
        img = Image.open(io.BytesIO(pix.tobytes("png")))
        try:
            text = pytesseract.image_to_string(img)
        except Exception as te:
            logger.warning(f"Tesseract OCR fallback error on page {page_num + 1}: {te}. Extracting direct text...")
            text = page.get_text()
        if text.strip():
            ocr_pages.append(f"## Page {page_num + 1}\n\n{text}")
            
    doc.close()
    return "\n\n".join(ocr_pages)

# ---------------------------------------------------------
# B. PHASE 3: DOWNLOAD & PHASE 4: CONVERT (WITH OCR)
# ---------------------------------------------------------
def download_pdf(article_id: str, pdf_url: str) -> Optional[Path]:
    """Downloads the PDF if not already present on disk."""
    if not pdf_url or pdf_url.lower() == "url":
        logger.error(f"[{article_id}] Invalid or missing PDF URL.")
        return None

    pdf_path = PDF_DIR / f"{article_id}.pdf"
    if pdf_path.exists() and pdf_path.stat().st_size > 1000:
        logger.info(f"[{article_id}] Cache hit: PDF exists on disk.")
        return pdf_path

    try:
        headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64)'}
        target_url = pdf_url

        # If URL does not have a galley id (e.g. /article/view/123 or /article/download/123), resolve citation_pdf_url from landing page
        if target_url.endswith(".pdf") or (target_url.count("/") > 7 and "/download/" in target_url):
            pass  # Already direct download URL
        else:
            view_url = target_url.replace("/article/download/", "/article/view/")
            logger.info(f"[{article_id}] Fetching landing page {view_url} to resolve citation_pdf_url...")
            r_view = requests.get(view_url, headers=headers, timeout=25)
            if r_view.ok:
                from bs4 import BeautifulSoup
                soup = BeautifulSoup(r_view.text, "html.parser")
                meta_pdf = soup.find("meta", attrs={"name": "citation_pdf_url"})
                if meta_pdf and meta_pdf.get("content"):
                    target_url = meta_pdf["content"]
                    logger.info(f"[{article_id}] Resolved citation_pdf_url: {target_url}")

        logger.info(f"[{article_id}] Downloading PDF from {target_url}...")
        response = requests.get(target_url, headers=headers, stream=True, timeout=25, allow_redirects=True)
        response.raise_for_status()

        # Check if response returned HTML landing page rather than raw binary PDF
        content_type = response.headers.get("Content-Type", "").lower()
        if "html" in content_type:
            from bs4 import BeautifulSoup
            from urllib.parse import urljoin
            soup = BeautifulSoup(response.text, "html.parser")
            meta_pdf = soup.find("meta", attrs={"name": "citation_pdf_url"})
            direct_pdf = meta_pdf["content"] if (meta_pdf and meta_pdf.get("content")) else None
            if not direct_pdf:
                tag = soup.find("a", href=lambda h: h and ('/article/download/' in h or h.endswith('.pdf')))
                if tag and tag.get("href"):
                    direct_pdf = urljoin(target_url, tag["href"])
            if direct_pdf:
                logger.info(f"[{article_id}] Resolved landing page to direct PDF: {direct_pdf}")
                response = requests.get(direct_pdf, headers=headers, stream=True, timeout=25, allow_redirects=True)
                response.raise_for_status()
        
        with open(pdf_path, 'wb') as f:
            for chunk in response.iter_content(chunk_size=8192):
                f.write(chunk)
                
        if pdf_path.exists() and pdf_path.stat().st_size > 500:
            time.sleep(1.0)  # Rate pacing
            return pdf_path
        else:
            logger.warning(f"[{article_id}] Downloaded file too small ({pdf_path.stat().st_size if pdf_path.exists() else 0} bytes).")
            return None
    except Exception as e:
        logger.error(f"[{article_id}] Download failed: {e}")
        return None

def convert_to_markdown(article_id: str, pdf_path: Path) -> Optional[str]:
    """
    Converts PDF to Markdown and caches it as markdown_texts/{article_id}.md.
    If digital text is < 500 chars (scanned/image-based), automatically falls back to OCR.
    """
    md_path = MD_DIR / f"{article_id}.md"
    
    # 1. Cache hit: Read directly from markdown cache
    if md_path.exists():
        logger.info(f"[{article_id}] Cache hit: Loading Markdown directly from disk.")
        return md_path.read_text(encoding='utf-8')

    try:
        logger.info(f"[{article_id}] Extracting Markdown via PyMuPDF4LLM...")
        md_text = pymupdf4llm.to_markdown(str(pdf_path))
        
        # 2. Scanned / Image-Based PDF Detection
        if len(md_text.strip()) < 500:
            logger.warning(f"[{article_id}] Text < 500 chars. PDF is image-based. Routing to OCR...")
            md_text = ocr_fallback_pdf(pdf_path)
            
        # 3. Final Quality Gate
        if len(md_text.strip()) < 200:
            logger.warning(f"[{article_id}] SKIP-LOW-QUALITY: Unreadable even after OCR.")
            return None
            
        # Save individual markdown file
        md_path.write_text(md_text, encoding='utf-8')
        return md_text
    except Exception as e:
        logger.error(f"[{article_id}] Markdown conversion failed: {e}")
        return None

# ---------------------------------------------------------
# PHASE 5: MULTI-AGENT SEMANTIC AUDIT
# ---------------------------------------------------------
def run_multi_agent_audit(metadata: dict, raw_markdown: str) -> BioethicsRecord:
    article_id = metadata["article_id"]
    t0 = time.time()
    # Trim markdown for prompt if exceptionally large while preserving first 15k chars for ethics
    sample_text = raw_markdown[:18000] if len(raw_markdown) > 18000 else raw_markdown
    logger.info(f"Auditing Article {article_id} ({len(raw_markdown)} chars) | '{metadata['article_title'][:45]}...'")
    
    # ---------------------------------------------------------
    # AGENT 1: GEMINI (The Extractor) - Forced JSON
    # ---------------------------------------------------------
    prompt = f"""
    You are a clinical data extractor. Read this markdown article and output a strict JSON object mapped exactly to these keys:
    - "study_design" (e.g., cross_sectional, RCT, observational, case_series, review, editorial)
    - "ethical_approval_status" (approved, waived, no_approval_stated)
    - "ethics_committee_name" (exact name or 'Not Reported')
    - "ethics_ref_number" (exact reference/protocol number or 'Not Reported')
    - "verbatim_ethics_quote" (verbatim sentence or 'Not Reported')
    - "ethics_page" (section name where found, e.g., 'Methods', 'Ethical Approval', or 'Not Reported')
    - "informed_consent_status" (written_consent, verbal_consent, waived, yes_unspecified, not_reported)
    - "informed_consent_evidence" (exact quote, or 'Not Reported')
    - "informed_consent_page" (section name, or 'Not Reported')
    
    Focus ONLY on empirical methods and ethics. Ignore references.
    ARTICLE TEXT:
    {sample_text}
    """
    
    def gemini_task(client):
        response = client.models.generate_content(
            model='gemini-3.5-flash-lite',
            contents=prompt,
            config={"response_mime_type": "application/json"}
        )
        return json.loads(response.text)

    try:
        evidence_json = executor.execute_with_retry("GEMINI", gemini_task)
    except Exception as ge:
        logger.warning(f"[GEMINI] Quota exhausted or error ({ge}). Falling back to GROQ for extraction...")
        telemetry.fallbacks_triggered += 1
        def groq_extract_task(client):
            response = client.chat.completions.create(
                model="openai/gpt-oss-120b",
                messages=[
                    {"role": "system", "content": "You are a clinical data extractor. Output ONLY strict JSON."},
                    {"role": "user", "content": prompt}
                ],
                response_format={"type": "json_object"}
            )
            return json.loads(response.choices[0].message.content)
        evidence_json = executor.execute_with_retry("GROQ", groq_extract_task)
    
    defaults = ["ethics_committee_name", "ethics_ref_number", "verbatim_ethics_quote", "ethics_page", "informed_consent_evidence", "informed_consent_page"]
    for key in defaults:
        if key not in evidence_json or not evidence_json[key]:
            evidence_json[key] = "Not Reported"
            
    if "informed_consent_status" not in evidence_json:
        evidence_json["informed_consent_status"] = "not_reported"

    # ---------------------------------------------------------
    # OPTIMIZATION POINT 1: EARLY-EXIT EXEMPTION
    # ---------------------------------------------------------
    design = evidence_json.get("study_design", "").lower()
    is_empirical = not any(x in design for x in ["review", "editorial", "commentary", "meta-analysis", "case_report"])
    
    if not is_empirical:
        logger.info(f"  -> Article {article_id} routed to EXEMPT (Early-Exit Pre-Triage).")
        evidence_json["ai_tier_classification"] = "Exempt: Non-Empirical/Case"
    else:
        # ---------------------------------------------------------
        # AGENT 2: CEREBRAS / GROQ (Bioethics Reviewer)
        # ---------------------------------------------------------
        def cerebras_task(client):
            # Model identifier on Cerebras is gpt-oss-120b (without openai/ prefix)
            model_name = "gpt-oss-120b" if getattr(client, "base_url", None) and "cerebras" in str(client.base_url) else "openai/gpt-oss-120b"
            response = client.chat.completions.create(
                model=model_name, 
                messages=[
                    {"role": "system", "content": "You are a Bioethics Reviewer. Adjudicate this extracted bioethics JSON evidence. Are the consent and IRB declarations legally sound and explicit?"},
                    {"role": "user", "content": json.dumps(evidence_json)}
                ]
            )
            return response.choices[0].message.content
        cerebras_review = executor.execute_with_retry("CEREBRAS", cerebras_task)

        # ---------------------------------------------------------
        # FAST MODE BYPASS (Skip Agent 3 if text < 8000 chars)
        # AGENT 3: GROQ (Adversarial Validator)
        # ---------------------------------------------------------
        if len(sample_text) < 8000:
            logger.info(f"  -> Article {article_id} < 8000 chars ({len(sample_text)}c). Fast Mode: Groq Adversary bypassed.")
        else:
            def groq_task(client):
                response = client.chat.completions.create(
                    model="openai/gpt-oss-20b", 
                    messages=[
                        {"role": "system", "content": "Devil's Advocate. Does this evidence strictly prove IRB and consent? Reply 'FLAG' if evidence is weak or contradictory, otherwise 'CLEAR'."},
                        {"role": "user", "content": json.dumps(evidence_json)}
                    ]
                )
                return response.choices[0].message.content
            
            validation = executor.execute_with_retry("GROQ", groq_task)
            if "FLAG" in validation.upper():
                logger.warning(f"  [!] Adversarial Flag raised for Article {article_id}")

        evidence_json["ai_tier_classification"] = determine_final_tier(evidence_json)

    latency = round(time.time() - t0, 2)
    telemetry.article_latencies.append(latency)
    tier = evidence_json.get("ai_tier_classification", "Tier 3: Silent Omission")
    telemetry.tier_counts[tier] = telemetry.tier_counts.get(tier, 0) + 1
    logger.info(f"  [OK] Audited {article_id} in {latency}s -> Tier: [{tier}]")

    # ---------------------------------------------------------
    # FINAL PYDANTIC VALIDATION AGAINST 16-COLUMN TEMPLATE
    # ---------------------------------------------------------
    try:
        record = BioethicsRecord(
            article_id=metadata["article_id"],
            article_title=metadata["article_title"],
            authors=metadata["authors"],
            journal_name=metadata["journal_name"],
            year=int(metadata["year"]),
            study_design=evidence_json.get("study_design", "Unknown"),
            ethical_approval_status=evidence_json.get("ethical_approval_status", "not_stated"),
            ethics_committee_name=evidence_json.get("ethics_committee_name", "Not Reported"),
            ethics_ref_number=evidence_json.get("ethics_ref_number", "Not Reported"),
            verbatim_ethics_quote=evidence_json.get("verbatim_ethics_quote", "Not Reported"),
            ethics_page=evidence_json.get("ethics_page", "Not Reported"),
            informed_consent_status=evidence_json.get("informed_consent_status", "not_reported"),
            informed_consent_evidence=evidence_json.get("informed_consent_evidence", "Not Reported"),
            informed_consent_page=evidence_json.get("informed_consent_page", "Not Reported"),
            ai_tier_classification=tier,
            pdf_url=metadata["pdf_url"]
        )
        return record
    except Exception as e:
        logger.error(f"Pydantic Schema Validation Failed for {article_id}: {e}")
        raise e

# ---------------------------------------------------------
# CLI ARGUMENTS
# ---------------------------------------------------------
def parse_args():
    parser = argparse.ArgumentParser(description="Headless Bioethics Audit Pipeline Master CLI")
    group = parser.add_mutually_exclusive_group(required=False)
    group.add_argument("--url", default="https://www.journalrmc.com/index.php/JRMC/issue/archive", help="Single journal archive URL")
    group.add_argument("--file", help="File with journal URLs")
    
    parser.add_argument("--year-from", type=int, default=2017, help="Start year (default: 2017)")
    parser.add_argument("--year-to", type=int, default=2026, help="End year (default: 2026)")
    parser.add_argument("--offset", type=int, default=0, help="Offset index to start processing from (default: 0)")
    parser.add_argument("--limit", type=int, default=20, help="Limit total articles (default: 20)")
    parser.add_argument("--output", default="output/Bioethics_Audit_Master.csv", help="Master output CSV path")
    return parser.parse_args()

def main():
    args = parse_args()
    logger.info("=" * 60)
    logger.info("MASTER BIOETHICS AUDIT PIPELINE — HEADLESS CLI RUN")
    logger.info("=" * 60)
    target_needed = args.offset + (args.limit if args.limit else 50)
    logger.info(f"Target URL: {args.url}")
    logger.info(f"Year Filter: {args.year_from} - {args.year_to} | Offset: {args.offset} | Limit: {args.limit} (Target: >= {target_needed})")
    
    output_path = Path(args.output)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    
    # Initialize SQLite Database
    init_db()
    
    # ---------------------------------------------------------
    # PHASE 1 & 2: DYNAMIC DISCOVERY & METADATA
    # ---------------------------------------------------------
    logger.info("\n--> Phase 1: Dynamic Discovery & Scraping Map Resolution...")
    scraping_map_path = Path("scraping_maps/journalrmc.com.json")
    smap = {}
    if scraping_map_path.exists():
        try:
            with open(scraping_map_path, "r", encoding="utf-8") as f:
                smap = json.load(f)
            logger.info(f"Loaded dynamic scraping map from {scraping_map_path}")
        except Exception as e:
            logger.warning(f"Could not load scraping map: {e}")

    crawler = JournalCrawler(scraping_map=smap)
    logger.info("Discovering journal issues matching year filter...")
    discovered_issues = crawler.discover_issues(args.url, max_pages=3)
    
    target_issues = [
        iss for iss in discovered_issues 
        if iss.get("year") and args.year_from <= int(iss["year"]) <= args.year_to
    ]
    logger.info(f"Found {len(discovered_issues)} total issues, {len(target_issues)} matched {args.year_from}-{args.year_to}.")
    
    if not target_issues:
        logger.warning("No target issues found by filter. Using first discovered issues.")
        target_issues = discovered_issues[:4]

    # Crawl articles from target issues until target_needed is met
    crawled_articles = []
    for issue in target_issues:
        if len(crawled_articles) >= target_needed:
            break
        logger.info(f"Crawling articles from issue: {issue.get('title', issue.get('url'))}...")
        arts = crawler.crawl_articles_from_issue(issue, journal_name="JRMC")
        for art in arts:
            if not art.get("pdf_url") and art.get("article_page_url"):
                page_url = art["article_page_url"]
                if "/article/view/" in page_url:
                    art["pdf_url"] = page_url.replace("/article/view/", "/article/download/")
                else:
                    art["pdf_url"] = page_url
            crawled_articles.append(art)
            if len(crawled_articles) >= target_needed:
                break

    logger.info(f"\n--> Phase 2: Metadata Assembly & ID Generation ({len(crawled_articles)} articles discovered)...")
    scraped_articles_metadata = []
    for idx, art in enumerate(crawled_articles, 1):
        year_val = art.get("year") or "2017"
        vol_val = art.get("volume") or "21"
        iss_val = art.get("issue") or "01"
        art_id = f"JRMC_{year_val}_V{vol_val}_I{iss_val}_{idx:03d}"
        
        meta = {
            "article_id": art_id,
            "article_title": art.get("title", f"Study Article {idx}"),
            "authors": art.get("authors", "Medical Research Group"),
            "journal_name": "Journal of Rawalpindi Medical College",
            "year": int(year_val),
            "pdf_url": art.get("pdf_url") or f"https://www.journalrmc.com/index.php/JRMC/article/download/{idx}/pdf"
        }
        scraped_articles_metadata.append(meta)

    # Apply pagination slice with offset and limit
    queued = scraped_articles_metadata[args.offset : args.offset + args.limit if args.limit else None]
    logger.info(f"Discovered: {len(scraped_articles_metadata)} articles | Queued (offset={args.offset}, limit={args.limit}): {len(queued)} articles.")

    # ---------------------------------------------------------
    # C. LIVE EXECUTION LOOP HOOK (main)
    # ---------------------------------------------------------
    logger.info(f"\n--> Live Pipeline Execution Loop on {len(queued)} Articles (Articles #{args.offset + 1} to #{args.offset + len(queued)})...")
    audited_records: List[BioethicsRecord] = []
    success_count = 0
    error_count = 0

    for i, meta in enumerate(queued, 1):
        art_id = meta["article_id"]
        pdf_url = meta.get("pdf_url", "")
        global_idx = args.offset + i
        logger.info(f"\n[{i}/{len(queued)}] (Global #{global_idx}) Starting processing for {art_id}...")

        # 1. download_pdf()
        pdf_path = download_pdf(art_id, pdf_url)
        if not pdf_path:
            error_count += 1
            logger.error(f"[{art_id}] Failed at PDF download step.")
            total_processed = success_count + error_count
            if total_processed >= 5 and (error_count / total_processed) > 0.30:
                logger.critical(f"Circuit breaker tripped: Error rate ({error_count}/{total_processed} = {error_count/total_processed:.1%}) exceeded 30% threshold. Halting pipeline.")
                break
            continue

        # 2. convert_to_markdown() (with OCR fallback)
        raw_markdown = convert_to_markdown(art_id, pdf_path)
        if not raw_markdown:
            error_count += 1
            logger.error(f"[{art_id}] Failed at Markdown conversion / quality gate.")
            total_processed = success_count + error_count
            if total_processed >= 5 and (error_count / total_processed) > 0.30:
                logger.critical(f"Circuit breaker tripped: Error rate ({error_count}/{total_processed} = {error_count/total_processed:.1%}) exceeded 30% threshold. Halting pipeline.")
                break
            continue

        # 3. run_multi_agent_audit()
        try:
            record = run_multi_agent_audit(meta, raw_markdown)
            audited_records.append(record)

            # 4. SQLite upsert
            upsert_success = upsert_audit_record(record.model_dump())
            if upsert_success:
                logger.info(f"[{art_id}] Successfully upserted to SQLite database.")
            success_count += 1
        except Exception as e:
            error_count += 1
            logger.error(f"[{art_id}] Multi-agent audit or persistence failed: {e}")

        # Check 30% circuit breaker after each item
        total_processed = success_count + error_count
        if total_processed >= 5 and (error_count / total_processed) > 0.30:
            logger.critical(f"Circuit breaker tripped: Error rate ({error_count}/{total_processed} = {error_count/total_processed:.1%}) exceeded 30% threshold. Halting pipeline.")
            break

    telemetry.end_time = time.time()
    total_duration = round(telemetry.end_time - telemetry.start_time, 2)

    # ---------------------------------------------------------
    # PHASE 6: COMPILE MASTER CSV & REPORT
    # ---------------------------------------------------------
    if audited_records:
        logger.info(f"\n--> Compiling Master 16-Column Audit Dataset to {output_path}...")
        import pandas as pd
        new_df = pd.DataFrame([rec.model_dump() for rec in audited_records])
        if output_path.exists():
            try:
                existing_df = pd.read_csv(output_path)
                df = pd.concat([existing_df, new_df], ignore_index=True).drop_duplicates(subset=["article_id"], keep="last")
            except Exception:
                df = new_df
        else:
            df = new_df
        df.to_csv(output_path, index=False, encoding="utf-8-sig")
        logger.info(f"Successfully saved {len(df)} total records to {output_path}")

    print("\n" + "=" * 70)
    print("           MASTER BIOETHICS AUDIT EXECUTION REPORT")
    print("=" * 70)
    print(f"Total Execution Duration : {total_duration} seconds ({total_duration/60:.2f} mins)")
    print(f"Total Successes          : {success_count}")
    print(f"Total Errors             : {error_count}")
    if (success_count + error_count) > 0:
        rate = (error_count / (success_count + error_count)) * 100
        print(f"Error Rate               : {rate:.1f}% (Threshold: 30%)")
    print(f"Articles Audited         : {len(audited_records)}")
    avg_lat = round(sum(telemetry.article_latencies) / max(1, len(telemetry.article_latencies)), 2)
    print(f"Average Audit Latency    : {avg_lat}s per article")
    print("\n[AI MODELS & API USAGE]")
    print(f"  • Gemini (gemini-3.5-flash-lite)  : {telemetry.api_calls['GEMINI']} calls")
    print(f"  • Cerebras (Reviewer / Fallback)   : {telemetry.api_calls['CEREBRAS']} calls")
    print(f"  • Groq (openai/gpt-oss-120b & 20b): {telemetry.api_calls['GROQ']} calls")
    print(f"  • Automatic Fallbacks Handled     : {telemetry.fallbacks_triggered}")
    
    print("\n[BIOETHICS TIER DISTRIBUTION]")
    for tier, count in sorted(telemetry.tier_counts.items()):
        pct = (count / max(1, len(audited_records))) * 100
        print(f"  • {tier:30s}: {count:2d} ({pct:5.1f}%)")
    print("=" * 70 + "\n")

if __name__ == "__main__":
    main()
