import csv
import requests
from datetime import datetime
import time
import streamlit as st
from collections import defaultdict, Counter

SCOPUS_API_KEY = st.secrets["SCOPUS_API_KEY"]

def parse_csv_to_dicts(file_obj):
    file_obj.seek(0)
    return list(csv.DictReader(file_obj))

def extract_clean_pmids(author_records):
    for row in author_records:
        row['PubMed_clean'] = row.get('PubMed', '').strip()
        row['MaxPR_PubMed_clean'] = row.get('MaxPR_PubMed', '').strip()
        row['EuropePMC_clean'] = row.get('EuropePMC', '').strip()

        row['PMID Final'] = (
            row['PubMed_clean'] or
            row['MaxPR_PubMed_clean'] or
            row['EuropePMC_clean']
        )
    return author_records

def call_scopus_api(scopus_ids):
    headers = {
        "Accept": "application/json",
        "X-ELS-APIKey": SCOPUS_API_KEY
    }
    results = []
    for sid in scopus_ids:
        if not sid:
            continue
        url = f"https://api.elsevier.com/content/abstract/eid/{sid}"
        try:
            response = requests.get(url, headers=headers)
            if response.status_code == 200:
                data = response.json().get('abstracts-retrieval-response', {})
                coredata = data.get('coredata', {})
                authors = data.get('authors', {}).get('author', [])
                if isinstance(authors, dict):
                    authors = [authors]

                author_full = [
                    f"{a.get('ce:given-name', '')} {a.get('ce:surname', '')}".strip()
                    for a in authors
                ]
                author_init = [a.get('ce:indexed-name', '').strip() for a in authors]
                author_ids = [a.get('@auid') for a in authors]
                author_seq = [a.get('@seq') for a in authors]

                pub_date = data.get('item', {}).get('bibrecord', {}).get('head', {}).get('source', {}).get('publicationdate', {})
                year = pub_date.get('year')
                month = pub_date.get('month')
                day = pub_date.get('day')

                results.append({
                    "Scopus ID": sid,
                    "Title": coredata.get("dc:title"),
                    "Authors Full": "; ".join(author_full),
                    "Authors Initial": ", ".join(author_init),
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
                    "Abstract": coredata.get("dc:description"),
                })
            time.sleep(0.5)
        except Exception:
            continue
    return results

def combine_date(year, month, day):
    try:
        month = '01' if not month else str(month).zfill(2)
        day = '01' if not day else str(day).zfill(2)
        return datetime.strptime(f"{month}/{day}/{year}", "%m/%d/%Y").date()
    except:
        return None

def is_peer_reviewed(doc_subtype):
    return doc_subtype in {"Article", "Book Chapter", "Review", "Short Survey"}

def flag_author_position(claimed_str, author_ids_str):
    claimed_set = set(claimed_str.split(';')) if claimed_str else set()
    paper_list = [a.strip() for a in author_ids_str.split(';') if a.strip()]

    if not paper_list:
        return False, False, False

    if len(paper_list) == 1:
        first = paper_list[0] in claimed_set
        return first, False, False

    first = paper_list[0] in claimed_set
    last = paper_list[-1] in claimed_set
    middle = any(a in claimed_set for a in paper_list[1:-1])
    return first, last, middle

def run_all(author_csv, publication_csv, start_date, end_date):
    df_1 = extract_clean_pmids(parse_csv_to_dicts(author_csv))
    df_2 = parse_csv_to_dicts(publication_csv)
    scopus_ids = list({row.get("Scopus", "").strip() for row in df_1 if row.get("Scopus")})
    df_3 = call_scopus_api(scopus_ids)

    # Build merged publication data
    df_6 = []
    for pub_row in df_2:
        username = pub_row.get("Username")
        author_data = next((a for a in df_1 if a.get("NetID") == username), {})
        scopus_data = next((s for s in df_3 if s.get("Scopus ID") == pub_row.get("Scopus")), {})

        combined = {**pub_row, **author_data, **scopus_data}

        pub_date = combine_date(
            scopus_data.get("Publication Year"),
            scopus_data.get("Publication Month"),
            scopus_data.get("Publication Day"),
        )
        combined["Publication Date"] = pub_date
        combined["Is Peer-Reviewed"] = is_peer_reviewed(scopus_data.get("Document SubType"))
        combined["Is_First_Author"], combined["Is_Last_Author"], combined["Is_Middle_Author"] = flag_author_position(
            author_data.get("ClaimedScopus", ""), scopus_data.get("Author IDs", "")
        )
        df_6.append(combined)

    # Filter peer-reviewed pubs in date range
    df_7 = [
        row for row in df_6
        if row.get("Is Peer-Reviewed")
        and row.get("Publication Date") is not None
        and start_date <= row["Publication Date"] <= end_date
    ]

    # Summary publication count per user
    df_8 = Counter(row.get("Username") for row in df_7 if row.get("DOI"))
    summary_df_8 = [{"Username": user, "Total_Pubs": count} for user, count in df_8.items()]

    # Rank-based assignments
    rank_order = [
        "Professor", "Research Professor", "Professor, Clinical", "Associate Professor",
        "Research Associate Professor", "Associate Professor, Clinical", "Assistant Professor",
        "Research Assistant Professor", "Assistant Professor, Clinical", "Instructor",
        "Instructor, Clinical", "Lecturer", "Adjunct Professor", "Adjunct Associate Professor",
        "Adjunct Assistant Professor", "Adjunct Instructor", "Adjunct Lecturer", "Professor Emeritus"
    ]
    position_rank = {title: i for i, title in enumerate(rank_order)}

    doi_to_author = {}
    for row in df_7:
        if row.get("Position_x") not in position_rank or not row.get("DOI"):
            continue
        rank = position_rank[row["Position_x"]]
        doi = row["DOI"]
        if doi not in doi_to_author or rank < doi_to_author[doi]["rank"]:
            doi_to_author[doi] = {"Username": row["Username"], "rank": rank, "details": row}

    assigned_counts = Counter(v["Username"] for v in doi_to_author.values())
    summary_df_11 = []
    for username, count in assigned_counts.items():
        details = next((v["details"] for v in doi_to_author.values() if v["Username"] == username), {})
        summary_df_11.append({
            "Username": username,
            "Computed Name Abbreviated": details.get("Computed Name Abbreviated", ""),
            "Position_x": details.get("Position_x", ""),
            "Total Publications": count
        })

    return summary_df_8, summary_df_11
