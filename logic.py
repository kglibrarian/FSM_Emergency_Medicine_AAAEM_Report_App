
import pandas as pd
import re
import requests
from datetime import datetime
import time
import streamlit as st
SCOPUS_API_KEY = st.secrets["SCOPUS_API_KEY"]

def run_all(df_1, df_2, api_key, start_date, end_date):
    ### Step 1: Clean PMIDs
    df_1['PubMed_clean'] = df_1['PubMed'].astype('Int64').astype('string')
    df_1['MaxPR_PubMed_clean'] = df_1['MaxPR_PubMed'].astype(str).str.extract(r'(\\d+)', expand=False)
    df_1['EuropePMC_clean'] = df_1['EuropePMC'].astype(str).str.extract(r'(\\d+)', expand=False)
    df_1['PMID Final'] = df_1['PubMed_clean'].combine_first(df_1['MaxPR_PubMed_clean']).combine_first(df_1['EuropePMC_clean'])

    # Extract unique Scopus IDs
    unique_scopus_id_list = df_1['Scopus'].dropna().astype(str).unique().tolist()

    # Query Scopus API
    headers = {
        "Accept": "application/json",
        "X-ELS-APIKey": api_key
    }
    results = []
    for sid in unique_scopus_id_list:
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
        except Exception as e:
            continue
        time.sleep(0.5)

    df_3 = pd.DataFrame(results)

    # Combine to master dataframe
    df_4 = df_2.merge(df_1, left_on='Username', right_on='NetID', how='left')
    df_5 = df_4.merge(df_3, left_on='Scopus', right_on='Scopus ID', how='left')
    df_6 = df_5.copy()

    # Flag authorship roles
    def flag_author_position(claimed_ids, paper_author_ids):
        if pd.isna(claimed_ids) or pd.isna(paper_author_ids):
            return pd.Series([False, False, False])
        
        claimed_set = set(claimed_ids.split(';'))
        paper_list = [a.strip() for a in paper_author_ids.split(';') if a.strip()]
        
        if not paper_list:
            return pd.Series([False, False, False])  # Prevent IndexError

        if len(paper_list) == 1:
            only_author = paper_list[0]
            first = only_author in claimed_set
            return pd.Series([first, False, False])
        
        first = paper_list[0] in claimed_set
        last = paper_list[-1] in claimed_set
        middle = any(a in claimed_set for a in paper_list[1:-1])
        
        return pd.Series([first, last, middle])

    valid_pub_mask = (
        df_6['DOI'].notna() |
        df_6['PMID Final'].notna() |
        df_6['Title'].notna()
    )

    df_6['Is_First_Author'] = False
    df_6['Is_Last_Author'] = False
    df_6['Is_Middle_Author'] = False

    df_6.loc[valid_pub_mask, ['Is_First_Author', 'Is_Last_Author', 'Is_Middle_Author']] = df_6[valid_pub_mask].apply(
        lambda row: flag_author_position(str(row['ClaimedScopus']), str(row['Author IDs'])), axis=1
    )

    df_6['Is Peer-Reviewed'] = df_6['Document SubType'].apply(lambda x: x in {'Article', 'Book Chapter', 'Review', 'Short Survey'} if pd.notna(x) else False)
    df_6['Publication Date'] = pd.to_datetime(df_6['Publication Date'], errors='coerce')
    df_7 = df_6[
        (df_6['Is Peer-Reviewed']) &
        (df_6['Publication Date'] >= start_date) &
        (df_6['Publication Date'] <= end_date)
    ].copy()

    df_8 = df_7.groupby("Username").agg(
        Total_Pubs=("DOI", lambda x: x.nunique())
    ).reset_index()

    # Create rank-based non-duplicated counts
    rank_order = [
        "Professor", "Research Professor", "Professor, Clinical", "Associate Professor",
        "Research Associate Professor", "Associate Professor, Clinical", "Assistant Professor",
        "Research Assistant Professor", "Assistant Professor, Clinical", "Instructor",
        "Instructor, Clinical", "Lecturer", "Adjunct Professor", "Adjunct Associate Professor",
        "Adjunct Assistant Professor", "Adjunct Instructor", "Adjunct Lecturer", "Professor Emeritus"
    ]
    position_rank = {title: rank for rank, title in enumerate(rank_order)}

    df_9 = df_7.copy()
    df_9["Rank"] = df_9["Position_x"].map(position_rank)
    df_9 = df_9[df_9["Position_x"].isin(position_rank)]
    df_10 = df_9.dropna(subset=["DOI"]).groupby("DOI").apply(
        lambda group: pd.Series({"Assigned To": group[group["Rank"] == group["Rank"].min()].iloc[0]["Username"]})
    ).reset_index()

    df_11 = df_10["Assigned To"].value_counts().reset_index()
    df_11.columns = ["Username", "Total Publications"]
    faculty_details = df_9[['Username', 'Computed Name Abbreviated', 'Position_x']].drop_duplicates()
    df_11 = faculty_details.merge(df_11, on="Username", how="left")
    df_11['Total Publications'] = df_11['Total Publications'].fillna(0).astype(int)
    df_11 = df_11[['Username', 'Computed Name Abbreviated', 'Position_x', 'Total Publications']]

    return df_8, df_11
