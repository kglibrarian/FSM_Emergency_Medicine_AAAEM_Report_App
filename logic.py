import csv
import io
import requests
import time
from datetime import datetime

def parse_csv_to_dicts(file_obj):
    return list(csv.DictReader(io.TextIOWrapper(file_obj, encoding="utf-8")))

def extract_clean_pmids(author_rows):
    for row in author_rows:
        row["PubMed_clean"] = row.get("PubMed", "").strip()
        row["MaxPR_PubMed_clean"] = row.get("MaxPR_PubMed", "").strip()
        row["EuropePMC_clean"] = row.get("EuropePMC", "").strip()
        row["PMID Final"] = row["PubMed_clean"] or row["MaxPR_PubMed_clean"] or row["EuropePMC_clean"]
    return author_rows

def get_unique_scopus_ids(rows):
    seen = set()
    for row in rows:
        sid = row.get("Scopus", "").strip()
        if sid:
            seen.add(sid)
    return list(seen)

def fetch_scopus_metadata(scopus_ids, api_key):
    headers = {
        "Accept": "application/json",
        "X-ELS-APIKey": api_key
    }
    results = []
    for sid in scopus_ids:
        url = f"https://api.elsevier.com/content/abstract/eid/{sid}"
        try:
            response = requests.get(url, headers=headers)
            if response.status_code == 200:
                data = response.json().get('abstracts-retrieval-response', {})
                coredata = data.get('coredata', {})
                authors = data.get('authors', {}).get('author', [])
                if isinstance(authors, dict):
                    authors = [authors]
                author_names_full = [
                    f"{a.get('ce:given-name', '')} {a.get('ce:surname', '')}".strip()
                    for a in authors if a.get('ce:given-name') or a.get('ce:surname')
                ]
                author_names_initial = [a.get('ce:indexed-name', '').strip() for a in authors]
                author_ids = [a.get('@auid') for a in authors]
                author_seq = [a.get('@seq') for a in authors]
                pub_date = data.get('item', {}).get('bibrecord', {}).get('head', {}).get('source', {}).get('publicationdate', {})
                year = pub_date.get('year')
                month = pub_date.get('month')
                day = pub_date.get('day')
                results.append({
                    "Scopus ID": sid,
                    "Title": coredata.get("dc:title"),
                    "Authors Full": "; ".join(author_names_full),
                    "Authors Initial": ", ".join(author_names_initial),
                    "Author IDs": "; ".join(author_ids),
                    "Author Sequence": "; ".join(author_seq),
                    "Publication Year": year,
                    "Publication Month": month,
                    "Publication Day": day,
                    "Document Type": coredata.get("prism:aggregationType"),
                    "Document SubType": coredata.get("subtypeDescription"),
                    "Source Title": coredata.get("prism:publicationName"),
                    "Volume": coredata.get("prism:volume"),
                    "Issue": coredata.get("prism:issueIdentifier"),
                    "Publisher": coredata.get("dc:publisher"),
                    "ISSN": coredata.get("prism:issn"),
                    "PMID": coredata.get("pubmed-id"),
                    "DOI": coredata.get("prism:doi"),
                    "Abstract": coredata.get("dc:description")
                })
        except Exception:
            continue
        time.sleep(0.5)
    return results

def run_all(author_csv_file, pub_csv_file, start_date, end_date):
    df_1 = extract_clean_pmids(parse_csv_to_dicts(author_csv_file))
    df_2 = parse_csv_to_dicts(pub_csv_file)

    # Example API key logic for testing; replace with streamlit secret in production
    import streamlit as st
    api_key = st.secrets["SCOPUS_API_KEY"]

    scopus_ids = get_unique_scopus_ids(df_1)
    scopus_data = fetch_scopus_metadata(scopus_ids, api_key)

    # Replace with real logic for df_8 and df_11
    df_8 = scopus_data  # Placeholder
    df_11 = df_1        # Placeholder

    return df_8, df_11
