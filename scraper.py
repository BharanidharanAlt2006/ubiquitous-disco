"""
DCSE Scholar — GitHub Actions Matrix Scraper
============================================
This script runs inside one of the 5 parallel GitHub Actions VMs.
It reads the full list of authors, determines its assigned chunk, and uses 
the scholarly library to fetch data directly from Google Scholar.

If Google blocks the Azure VM's IP, it self-heals by dynamically 
enabling scholarly's FreeProxy module and retrying.

Required environment variables (set via GitHub Secrets):
    AUTHOR_IDS      — Comma-separated list of ALL 30 Google Scholar author IDs
    WEBHOOK_URL     — Full URL to the webhook (e.g. http://server:8000/webhook/ingest)
    WEBHOOK_SECRET  — Shared secret for authenticating with the webhook
    CHUNK_INDEX     — Automatically passed by GitHub Actions (0 to 4)
"""

import os
import sys
import json
import time
import requests
import concurrent.futures
from datetime import datetime, timezone
from scholarly import scholarly, ProxyGenerator

# ---------------------------------------------------------------------------
# Configuration (read from environment)
# ---------------------------------------------------------------------------

AUTHOR_IDS_RAW = os.environ.get("AUTHOR_IDS", "")
WEBHOOK_URL = os.environ.get("WEBHOOK_URL", "")
WEBHOOK_SECRET = os.environ.get("WEBHOOK_SECRET", "")
CHUNK_INDEX_STR = os.environ.get("CHUNK_INDEX", "0")

# Retry config for the webhook POST
WEBHOOK_MAX_RETRIES = 3
WEBHOOK_RETRY_DELAY_SECONDS = 10

# Scraping limits
MAX_RETRIES_PER_AUTHOR = 8
MAX_SECOND_PASS_RETRIES = 3


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def validate_env():
    """Ensure all required environment variables are present."""
    missing = []
    if not AUTHOR_IDS_RAW:
        missing.append("AUTHOR_IDS")
    if not WEBHOOK_URL:
        missing.append("WEBHOOK_URL")
    if not WEBHOOK_SECRET:
        missing.append("WEBHOOK_SECRET")

    if missing:
        print(f"[FATAL] Missing required environment variables: {', '.join(missing)}")
        sys.exit(1)


def infer_pub_type_from_venue(venue_string):
    """
    Infer publication type from the venue/publication string returned by Google Scholar.
    """
    if not venue_string:
        return "unknown"

    text = venue_string.lower()

    conference_keywords = [
        "conference", "proceedings", "proc.", "workshop", "symposium",
        "icml", "neurips", "cvpr", "iccv", "eccv", "aaai", "ijcai",
        "acl", "emnlp", "naacl", "sigmod", "vldb", "icde", "kdd",
        "www", "chi", "uist", "infocom", "globecom", "icdcs",
    ]
    journal_keywords = [
        "journal", "transactions", "trans.", "letters", "review",
        "magazine", "ieee access", "plos", "nature", "science",
        "lancet", "annals", "archives", "bulletin",
    ]
    book_keywords = ["book", "springer", "wiley", "elsevier", "chapter"]

    if any(kw in text for kw in conference_keywords):
        return "conference"
    if any(kw in text for kw in journal_keywords):
        return "journal"
    if any(kw in text for kw in book_keywords):
        return "book"
    return "unknown"


# ---------------------------------------------------------------------------
# Core Scraping Logic with Self-Healing Proxy
# ---------------------------------------------------------------------------

_proxy_enabled = False

def enable_free_proxy():
    """
    Engages scholarly's FreeProxy system. 
    This is slow (it hunts for working free proxies), so we only use it if the direct connection gets blocked.
    """
    global _proxy_enabled
    print("\n  [!] Google blocked the direct IP. Self-healing initiated...")
    print("  [!] Hunting for a working Free Proxy. This might take 1-3 minutes...")
    
    pg = ProxyGenerator()
    # Scrapes free proxy sites, tests them, and assigns a working one
    success = pg.FreeProxies()
    
    if success:
        scholarly.use_proxy(pg)
        _proxy_enabled = True
        print("  [+] Free Proxy found and enabled! Retrying...")
        return True
    else:
        print("  [-] Failed to find a working Free Proxy.")
        return False


def fetch_author(author_id):
    """
    Fetches the author's profile and all their publications using scholarly.
    Attempts direct connection first. If it fails, falls back to FreeProxy automatically.
    """
    for attempt in range(1, MAX_RETRIES_PER_AUTHOR + 1):
        try:
            print(f"  Attempt {attempt}/{MAX_RETRIES_PER_AUTHOR} (Proxy Enabled: {_proxy_enabled})")
            
            # Execute all scholarly network requests inside the 60s timeout to defeat Tarpits
            executor = concurrent.futures.ThreadPoolExecutor(max_workers=1)
            try:
                # Step 1: Find the author by ID
                future_search = executor.submit(scholarly.search_author_id, author_id)
                search_query = future_search.result(timeout=60)
                
                # Step 2: Fill in the full profile
                future_fill = executor.submit(scholarly.fill, search_query, sections=['publications', 'indices'])
                author = future_fill.result(timeout=60)
            finally:
                # DO NOT wait for the stuck thread to finish
                executor.shutdown(wait=False)
            
            # Extract basic info
            name = author.get('name', 'Unknown')
            affiliation = author.get('affiliation', '')
            total_citations = author.get('citedby', 0)
            
            # Calculate citations since 2021
            citations_since_2021 = 0
            cites_per_year = author.get('cites_per_year', {})
            for year, count in cites_per_year.items():
                if int(year) >= 2021:
                    citations_since_2021 += count

            # Extract publications
            formatted_articles = []
            for pub in author.get('publications', []):
                bib = pub.get('bib', {})
                title = bib.get('title', 'Unknown Title')
                
                # Extract year
                year = None
                if 'pub_year' in bib and str(bib['pub_year']).isdigit():
                    year = int(bib['pub_year'])
                
                cited_by = pub.get('num_citations', 0)
                link = pub.get('pub_url', None)
                
                # Determine type from the venue string
                venue = bib.get('citation', '')
                pub_type = infer_pub_type_from_venue(venue)

                formatted_articles.append({
                    "title": title,
                    "year": year,
                    "cited_by": cited_by,
                    "pub_type": pub_type,
                    "link": link,
                })

            print(f"    Success: {name} | {len(formatted_articles)} publications")
            
            return {
                "author_id": author_id,
                "name": name,
                "affiliation": affiliation,
                "total_citations": total_citations,
                "citations_since_2021": citations_since_2021,
                "articles": formatted_articles,
            }

        except concurrent.futures.TimeoutError:
            print("    [WARN] Scrape timed out (>60s). Google is tarpitting this IP.")
            
            if attempt == MAX_RETRIES_PER_AUTHOR:
                print(f"    [ERROR] Max retries exhausted for {author_id} on this pass.")
                return None
            
            # If it's a network/block issue and we haven't enabled the proxy yet, do it now
            if not _proxy_enabled:
                success = enable_free_proxy()
                if not success:
                    print("    Waiting 10 seconds before retrying...")
                    time.sleep(10)
            else:
                # If we are already on a proxy and it failed, the proxy probably died.
                # Find a new one.
                print("    Proxy failed. Hunting for a new one...")
                success = enable_free_proxy()
                if not success:
                    print("    Waiting 10 seconds before retrying...")
                    time.sleep(10)

        except Exception as e:
            error_msg = str(e).lower()
            print(f"    [WARN] Scrape failed: {e}")
            
            if attempt == MAX_RETRIES_PER_AUTHOR:
                print(f"    [ERROR] Max retries exhausted for {author_id} on this pass.")
                return None
            
            # If it's a network/block issue and we haven't enabled the proxy yet, do it now
            if not _proxy_enabled:
                success = enable_free_proxy()
                if not success:
                    print("    Waiting 10 seconds before retrying...")
                    time.sleep(10)
            else:
                # If we are already on a proxy and it failed, the proxy probably died.
                # Find a new one.
                print("    Proxy failed. Hunting for a new one...")
                success = enable_free_proxy()
                if not success:
                    print("    Waiting 10 seconds before retrying...")
                    time.sleep(10)


# ---------------------------------------------------------------------------
# Webhook Delivery
# ---------------------------------------------------------------------------

def send_to_webhook(authors_data):
    """POST the scraped data to the department server's webhook endpoint."""
    payload = json.dumps(authors_data, ensure_ascii=False)
    headers = {
        "Content-Type": "application/json",
        "X-Webhook-Secret": WEBHOOK_SECRET,
    }

    for attempt in range(1, WEBHOOK_MAX_RETRIES + 1):
        print(f"\n[Webhook] Attempt {attempt}/{WEBHOOK_MAX_RETRIES}: Sending {len(authors_data)} authors to {WEBHOOK_URL}")

        try:
            response = requests.post(
                WEBHOOK_URL,
                data=payload,
                headers=headers,
                timeout=30,
            )

            if response.status_code == 200:
                result = response.json()
                print(f"[Webhook] Success! Server response: {result}")
                return True
            elif response.status_code == 403:
                print(f"[Webhook] FATAL: Authentication failed (403). Check WEBHOOK_SECRET.")
                return False
            else:
                print(f"[Webhook] Server returned {response.status_code}: {response.text}")

        except requests.exceptions.RequestException as e:
            print(f"[Webhook] Network error: {e}")

        if attempt < WEBHOOK_MAX_RETRIES:
            print(f"[Webhook] Retrying in {WEBHOOK_RETRY_DELAY_SECONDS}s...")
            time.sleep(WEBHOOK_RETRY_DELAY_SECONDS)

    print("[Webhook] FATAL: All retry attempts exhausted.")
    return False

def get_pending_authors(all_author_ids):
    """Ask the server which authors actually need scraping."""
    pending_url = WEBHOOK_URL.replace("/ingest", "/pending")
    headers = {
        "Content-Type": "application/json",
        "X-Webhook-Secret": WEBHOOK_SECRET,
    }
    payload = {"author_ids": all_author_ids}
    
    try:
        print(f"\n[Webhook] Requesting pending authors from {pending_url}...")
        response = requests.post(pending_url, json=payload, headers=headers, timeout=30)
        if response.status_code == 200:
            pending_ids = response.json().get("pending_ids", [])
            print(f"[Webhook] Server reports {len(pending_ids)} authors need scraping.")
            return pending_ids
        else:
            print(f"[Webhook] Failed to fetch pending authors. Status {response.status_code}")
            return all_author_ids
    except Exception as e:
        print(f"[Webhook] Error fetching pending authors: {e}")
        return all_author_ids


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def chunk_list(lst, num_chunks):
    """Yields successive chunks from lst."""
    # Calculates the base size and remainder to distribute evenly
    k, m = divmod(len(lst), num_chunks)
    return [lst[i * k + min(i, m):(i + 1) * k + min(i + 1, m)] for i in range(num_chunks)]


def main():
    print("=" * 60)
    print("DCSE Scholar — Matrix Scraper (Single Repo)")
    print(f"Timestamp: {datetime.now(timezone.utc).isoformat()}")
    print("=" * 60)

    validate_env()
    
    try:
        chunk_index = int(CHUNK_INDEX_STR)
    except ValueError:
        print(f"[FATAL] CHUNK_INDEX must be an integer, got: '{CHUNK_INDEX_STR}'")
        sys.exit(1)

    # Parse the FULL comma-separated list of author IDs
    all_author_ids = [aid.strip() for aid in AUTHOR_IDS_RAW.split(",") if aid.strip()]
    print(f"Total authors loaded from config: {len(all_author_ids)}")
    
    # Query server to find out who actually needs to be scraped
    pending_ids = get_pending_authors(all_author_ids)
    
    if not pending_ids:
        print("\n[+] All authors are fully up to date! Nothing to do for this machine.")
        sys.exit(0)
    
    # Split the pending list into 5 chunks
    chunks = chunk_list(pending_ids, 5)
    
    if chunk_index >= len(chunks):
        print(f"\n[+] This machine ({chunk_index}) is not needed for the remaining authors. Exiting cleanly.")
        sys.exit(0)
        
    my_assigned_authors = chunks[chunk_index]
    
    print(f"\n--- MACHINE {chunk_index} ---")
    print(f"Assigned {len(my_assigned_authors)} authors to this runner: {my_assigned_authors}")
    print(f"Webhook target: {WEBHOOK_URL}\n")

    success_count = 0
    failed_authors = []

    # --- FIRST PASS ---
    for i, author_id in enumerate(my_assigned_authors, 1):
        print(f"\n[{i}/{len(my_assigned_authors)}] Scraping: {author_id}")

        result = fetch_author(author_id)
        if result is not None:
            # Send to webhook immediately so data isn't lost if the script times out later
            webhook_success = send_to_webhook([result])
            if webhook_success:
                success_count += 1
            else:
                print(f"    [ERROR] Failed to send {author_id} to webhook.")
        else:
            failed_authors.append(author_id)
            
        # Polite delay between successful scrapes to avoid triggering blocks early
        time.sleep(2)

    # --- SECOND PASS (Sweep for failures) ---
    if failed_authors:
        print(f"\n{'=' * 40}")
        print(f"Initiating Second Pass for {len(failed_authors)} failed authors...")
        
        for author_id in failed_authors:
            print(f"\n[RETRY] Scraping: {author_id}")
            # Temporarily reduce retries for the sweep to avoid hitting 25min timeout
            global MAX_RETRIES_PER_AUTHOR
            MAX_RETRIES_PER_AUTHOR = MAX_SECOND_PASS_RETRIES
            
            result = fetch_author(author_id)
            if result is not None:
                webhook_success = send_to_webhook([result])
                if webhook_success:
                    success_count += 1
            time.sleep(2)

    # Summary
    print(f"\n{'=' * 60}")
    print(f"Scrape complete for Machine {chunk_index}.")
    print(f"Successfully scraped and sent: {success_count}/{len(my_assigned_authors)} authors")

    if success_count == 0:
        print("\n[FATAL] No authors were successfully sent. Exiting with error.")
        os._exit(1)

    print("\nDone.")
    # Forcefully kill any hanging non-daemon socket threads
    os._exit(0)

if __name__ == "__main__":
    main()
