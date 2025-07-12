import pandas as pd
import requests
import time
from datetime import datetime
import numpy as np


class PublicationAnalyzer:
    def __init__(self, scopus_api_key):
        self.scopus_api_key = scopus_api_key
        self.headers = {
            "Accept": "application/json",
            "X-ELS-APIKey": scopus_api_key
        }
        self.rate_limit_delay = 1.0
        self.max_retries = 3
        self.batch_size = 25
        
    def consolidate_pmids(self, df):
        """Consolidate PMIDs from multiple columns"""
        print(f"DEBUG: Input df shape: {df.shape}")
        
        # Clean and normalize columns - exact same as notebook
        df['PubMed_clean'] = df['PubMed'].astype('Int64').astype('string')
        df['MaxPR_PubMed_clean'] = df['MaxPR_PubMed'].str.extract(r'(\d+)', expand=False)
        df['EuropePMC_clean'] = df['EuropePMC'].str.extract(r'(\d+)', expand=False)
        
        # Create final PMID column with priority - exact same as notebook
        df['PMID Final'] = df['PubMed_clean'].combine_first(df['MaxPR_PubMed_clean']).combine_first(df['EuropePMC_clean'])
        
        print(f"DEBUG: PMIDs consolidated - non-null PMID Final: {df['PMID Final'].notna().sum()}")
        return df
    
    def query_scopus_api(self, scopus_ids, progress_callback=None):
        """Query Scopus API for publication metadata - simplified for debugging"""
        print(f"DEBUG: Starting API queries for {len(scopus_ids)} Scopus IDs")
        
        # For debugging, let's just query the first 5 to speed things up
        scopus_ids = scopus_ids[:5]
        print(f"DEBUG: Limiting to first 5 IDs for debugging: {scopus_ids}")
        
        results = []
        
        for sid in scopus_ids:
            url = f"https://api.elsevier.com/content/abstract/eid/{sid}"
            
            try:
                response = requests.get(url, headers=self.headers)
                
                if response.status_code == 200:
                    data = response.json().get('abstracts-retrieval-response', {})
                    coredata = data.get('coredata', {})
                    
                    authors = data.get('authors', {}).get('author', [])
                    if isinstance(authors, dict):
                        authors = [authors]
                    
                    author_ids = [a.get('@auid') for a in authors if a.get('@auid')]
                    
                    pub_date = (data.get('item', {})
                               .get('bibrecord', {})
                               .get('head', {})
                               .get('source', {})
                               .get('publicationdate', {}))
                    
                    year = pub_date.get('year')
                    month = pub_date.get('month')
                    day = pub_date.get('day')
                    
                    result = {
                        "Scopus ID": sid,
                        "Title": coredata.get("dc:title"),
                        "Author IDs": "; ".join(author_ids),
                        "Publication Year": year,
                        "Publication Month": month,
                        "Publication Day": day,
                        "Document SubType": coredata.get("subtypeDescription"),
                        "DOI": coredata.get("prism:doi"),
                    }
                    
                    results.append(result)
                    
                    print(f"DEBUG: API result for {sid}:")
                    print(f"  Title: {result['Title']}")
                    print(f"  Author IDs: {result['Author IDs']}")
                    print(f"  Document SubType: {result['Document SubType']}")
                    print(f"  Year: {result['Publication Year']}")
                    
                else:
                    print(f"DEBUG: API failed for {sid}: {response.status_code}")
                    
            except Exception as e:
                print(f"DEBUG: API error for {sid}: {str(e)}")
            
            time.sleep(self.rate_limit_delay)
        
        print(f"DEBUG: API queries complete - got {len(results)} results")
        return pd.DataFrame(results)
    
    def combine_date_parts(self, year, month, day):
        """Safely combine date parts into datetime"""
        try:
            month = '01' if not month or pd.isna(month) else str(month).zfill(2)
            day = '01' if not day or pd.isna(day) else str(day).zfill(2)
            return datetime.strptime(f"{month}/{day}/{year}", "%m/%d/%Y").date()
        except:
            return pd.NaT
    
    def process_data(self, df1, df2, start_date, end_date, status_callback=None):
        """Simplified processing pipeline for debugging"""
        
        def update_status(message):
            if status_callback:
                status_callback(message)
            print(message)
        
        # Step 1: Consolidate PMIDs
        update_status("🔄 Step 1: Consolidating PMIDs...")
        df1 = self.consolidate_pmids(df1)
        
        # Step 2: Get unique Scopus IDs and query API (limited for debugging)
        unique_scopus_ids = df1['Scopus'].dropna().astype(str).unique().tolist()
        update_status(f"🔍 Step 2: Found {len(unique_scopus_ids)} unique Scopus IDs")
        
        if not unique_scopus_ids:
            raise ValueError("No Scopus IDs found in the data!")
        
        df_scopus = self.query_scopus_api(unique_scopus_ids)
        
        # Step 3: Add publication dates
        update_status("📅 Step 3: Processing publication dates...")
        df_scopus['Publication Date'] = df_scopus.apply(
            lambda row: self.combine_date_parts(
                row['Publication Year'], 
                row['Publication Month'], 
                row['Publication Day']
            ), axis=1
        )
        
        print(f"DEBUG: Scopus data after dates:")
        print(df_scopus[['Scopus ID', 'Title', 'Author IDs', 'Document SubType', 'Publication Date']].head())
        
        # Step 4: Merge datasets
        update_status("🔗 Step 4: Merging datasets...")
        print(f"DEBUG: Before merging:")
        print(f"  df1 shape: {df1.shape}")
        print(f"  df2 shape: {df2.shape}")
        print(f"  df_scopus shape: {df_scopus.shape}")
        print(f"  df1 NetID unique: {df1['NetID'].nunique()}")
        print(f"  df2 Username unique: {df2['Username'].nunique()}")
        
        # First merge: df2 + df1
        df_merged = df2.merge(df1, left_on='Username', right_on='NetID', how='left')
        print(f"DEBUG: After first merge (df2 + df1): {df_merged.shape}")
        print(f"DEBUG: Non-null Scopus in merged: {df_merged['Scopus'].notna().sum()}")
        
        # Second merge: add Scopus data
        df_merged = df_merged.merge(df_scopus, left_on='Scopus', right_on='Scopus ID', how='left')
        print(f"DEBUG: After second merge (+ Scopus): {df_merged.shape}")
        print(f"DEBUG: Non-null Scopus ID in final: {df_merged['Scopus ID'].notna().sum()}")
        
        # Show sample of merged data
        sample_merged = df_merged[df_merged['Scopus ID'].notna()].head(3)
        if len(sample_merged) > 0:
            print(f"DEBUG: Sample merged data:")
            for idx, row in sample_merged.iterrows():
                print(f"  Username: {row['Username']}")
                print(f"  ClaimedScopus: {row['ClaimedScopus']}")
                print(f"  Author IDs: {row['Author IDs']}")
                print(f"  Document SubType: {row['Document SubType']}")
                print(f"  ---")
        
        # Step 5: Flag author positions - EXACT same as notebook
        update_status("👥 Step 5: Flagging author positions...")
        
        def flag_author_position(claimed_ids, paper_author_ids):
            if pd.isna(claimed_ids) or pd.isna(paper_author_ids):
                return pd.Series([False, False, False])
            
            claimed_set = set(claimed_ids.split(';'))
            paper_list = [a.strip() for a in paper_author_ids.split(';') if a.strip()]
            
            print(f"    DEBUG: Comparing claimed_set={claimed_set} vs paper_list={paper_list}")
            
            if len(paper_list) == 1:
                only_author = paper_list[0]
                first = only_author in claimed_set
                print(f"    DEBUG: Single author match: {first}")
                return pd.Series([first, False, False])
            
            first = paper_list[0] in claimed_set
            last = paper_list[-1] in claimed_set
            middle = any(a in claimed_set for a in paper_list[1:-1])
            
            print(f"    DEBUG: Multi-author matches: first={first}, last={last}, middle={middle}")
            
            return pd.Series([first, last, middle])
        
        # Define a mask for rows that appear to be valid publications
        valid_pub_mask = (
            df_merged['DOI'].notna() |
            df_merged['PMID Final'].notna() |
            df_merged['Title'].notna()
        )
        
        print(f"DEBUG: Valid publication mask: {valid_pub_mask.sum()} out of {len(df_merged)}")
        
        # Initialize all flags to False
        df_merged['Is_First_Author'] = False
        df_merged['Is_Last_Author'] = False
        df_merged['Is_Middle_Author'] = False
        
        # Apply author position flagging - manually for debugging
        valid_rows = df_merged[valid_pub_mask]
        print(f"DEBUG: Processing {len(valid_rows)} valid rows for author flagging...")
        
        for idx, row in valid_rows.head(10).iterrows():  # Just first 10 for debugging
            print(f"  DEBUG: Processing row {idx}, Username: {row['Username']}")
            
            result = flag_author_position(str(row['ClaimedScopus']), str(row['Author IDs']))
            df_merged.at[idx, 'Is_First_Author'] = result.iloc[0]
            df_merged.at[idx, 'Is_Last_Author'] = result.iloc[1]
            df_merged.at[idx, 'Is_Middle_Author'] = result.iloc[2]
        
        print(f"DEBUG: After author flagging:")
        print(f"  First author flags: {df_merged['Is_First_Author'].sum()}")
        print(f"  Last author flags: {df_merged['Is_Last_Author'].sum()}")
        print(f"  Middle author flags: {df_merged['Is_Middle_Author'].sum()}")
        
        # Step 6: Flag peer-reviewed publications
        update_status("📖 Step 6: Flagging peer-reviewed publications...")
        
        def flag_peer_reviewed(doc_subtype):
            peer_reviewed_types = {'Article', 'Book Chapter', 'Review', 'Short Survey'}
            if pd.isna(doc_subtype):
                return False
            return doc_subtype.strip() in peer_reviewed_types
        
        df_merged['Is Peer-Reviewed'] = df_merged['Document SubType'].apply(flag_peer_reviewed)
        
        print(f"DEBUG: Peer-reviewed publications: {df_merged['Is Peer-Reviewed'].sum()}")
        print(f"DEBUG: Document subtypes found: {df_merged['Document SubType'].value_counts()}")
        
        # Step 7: Filter by date range and peer-review status
        update_status("📊 Step 7: Filtering by date and peer-review...")
        
        # Convert the column to proper booleans
        df_merged['Is Peer-Reviewed'] = df_merged['Is Peer-Reviewed'].astype(str).str.upper().eq("TRUE")
        
        # Ensure proper datetime type
        df_merged['Publication Date'] = pd.to_datetime(df_merged['Publication Date'], errors='coerce')
        
        start_date_ts = pd.Timestamp(start_date)
        end_date_ts = pd.Timestamp(end_date)
        
        print(f"DEBUG: Date filtering from {start_date_ts} to {end_date_ts}")
        print(f"DEBUG: Publications with valid dates: {df_merged['Publication Date'].notna().sum()}")
        
        date_mask = (
            (df_merged['Publication Date'] >= start_date_ts) &
            (df_merged['Publication Date'] <= end_date_ts)
        )
        print(f"DEBUG: Publications in date range: {date_mask.sum()}")
        
        peer_reviewed_mask = df_merged['Is Peer-Reviewed']
        print(f"DEBUG: Peer-reviewed publications: {peer_reviewed_mask.sum()}")
        
        combined_mask = peer_reviewed_mask & date_mask
        print(f"DEBUG: Publications passing both filters: {combined_mask.sum()}")
        
        df_filtered = df_merged[combined_mask].copy()
        
        print(f"DEBUG: Final filtered dataset: {df_filtered.shape}")
        
        # Fix boolean-like flag columns before proceeding
        for col in ['Is_First_Author', 'Is_Last_Author', 'Is_Middle_Author', 'Is Peer-Reviewed']:
            df_filtered[col] = df_filtered[col].astype(str).str.upper().eq("TRUE")
        
        print(f"DEBUG: Final authorship flags in filtered data:")
        print(f"  First author: {df_filtered['Is_First_Author'].sum()}")
        print(f"  Last author: {df_filtered['Is_Last_Author'].sum()}")
        print(f"  Middle author: {df_filtered['Is_Middle_Author'].sum()}")
        
        # Show what we have in filtered data
        if len(df_filtered) > 0:
            print(f"DEBUG: Sample of filtered data:")
            sample = df_filtered[['Username', 'Title', 'Is_First_Author', 'Is_Last_Author', 'Is_Middle_Author']].head(3)
            print(sample.to_string())
        
        # Step 8: Generate simplified faculty summary
        update_status("📋 Step 8: Generating faculty summary...")
        
        # Get faculty list
        faculty_info = df_merged[['Computed Name Abbreviated', 'Username', 'Position_x', 'Arrive Date', 'Leave Date']].drop_duplicates()
        print(f"DEBUG: Faculty count: {len(faculty_info)}")
        
        # Simple any-position summary for debugging
        summary_rows = []
        for _, row in faculty_info.iterrows():
            netid = row['Username']
            person_summary = row.to_dict()
            
            # Check what publications this person has in filtered data
            person_pubs = df_filtered[df_filtered['Username'] == netid]
            
            any_position_count = len(person_pubs)
            any_position_pmids = person_pubs['PMID Final'].dropna().astype(str).tolist()
            
            person_summary.update({
                'Any_Position_PMIDs': ', '.join(any_position_pmids),
                'Any_Position_PMID_Count': any_position_count,
                'Any_Position_NonPMID_Count': 0,
                'Is_First_Author_PMIDs': '',
                'Is_First_Author_PMID_Count': 0,
                'Is_First_Author_NonPMID_Count': 0,
                'Is_Last_Author_PMIDs': '',
                'Is_Last_Author_PMID_Count': 0,
                'Is_Last_Author_NonPMID_Count': 0,
                'Is_Middle_Author_PMIDs': '',
                'Is_Middle_Author_PMID_Count': 0,
                'Is_Middle_Author_NonPMID_Count': 0,
                'Coauthor with Another Faculty': 'No'
            })
            
            if any_position_count > 0:
                print(f"DEBUG: {netid} has {any_position_count} publications")
            
            summary_rows.append(person_summary)
        
        df_faculty_summary = pd.DataFrame(summary_rows)
        
        # Rename columns to AAAEM template
        column_renames = {
            'Is_First_Author_PMIDs': 'PMIDs for all PubMed indexed publications during the past AY (authorship as first author)',
            'Is_First_Author_PMID_Count': 'Number of PubMed indexed publications during the past AY (authorship as first author)',
            'Is_First_Author_NonPMID_Count': 'Number of Non-PubMed indexed publications during the past AY (authorship as first author)',
            'Is_Last_Author_PMIDs': 'PMIDs for all PubMed indexed publications during the past AY (authorship as last author)',
            'Is_Last_Author_PMID_Count': 'Number of PubMed indexed publications during the past AY (authorship as last author)',
            'Is_Last_Author_NonPMID_Count': 'Number of Non-PubMed indexed publications during the past AY (authorship as last author)',
            'Is_Middle_Author_PMIDs': 'PMIDs for all PubMed indexed publications during the past AY (authorship in any position besides first or last)',
            'Is_Middle_Author_PMID_Count': 'Number of PubMed indexed publications during the past AY (authorship in any position besides first or last)',
            'Is_Middle_Author_NonPMID_Count': 'Number of Non-PubMed indexed publications during the past AY (authorship in any position besides first or last)',
            'Any_Position_PMIDs': 'PMIDs for all  PubMed indexed publications during the past AY (authorship in any position)',
            'Any_Position_PMID_Count': 'Number of PubMed indexed publications during the past AY (authorship in any position)',
            'Any_Position_NonPMID_Count': 'Number of Non-PubMed indexed publications during the past AY (authorship in any position)',
            'Coauthor with Another Faculty': 'Are there any overlapping publications (PMIDs or non-PMIDs) between faculty? If yes, do NOT double count them in the AAAEM metrics. See the instructions in the "AAAEM" worksheets.'
        }
        
        df_faculty_summary = df_faculty_summary.rename(columns=column_renames)
        
        print(f"DEBUG: Faculty with any position publications: {(df_faculty_summary['Number of PubMed indexed publications during the past AY (authorship in any position)'] > 0).sum()}")
        
        # Simple publication summary
        df_pub_summary = pd.DataFrame({
            'Username': ['test'],
            'Computed Name Abbreviated': ['Test'],
            'Position_x': ['Test'],
            'Total Publications': [0]
        })
        
        update_status("✅ Debug processing completed!")
        
        return df_faculty_summary, df_pub_summary