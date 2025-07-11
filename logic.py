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
        print(f"DEBUG: Input columns: {list(df.columns)}")
        
        # Clean and normalize columns
        df['PubMed_clean'] = df['PubMed'].astype('Int64').astype('string')
        df['MaxPR_PubMed_clean'] = df['MaxPR_PubMed'].str.extract(r'(\d+)', expand=False)
        df['EuropePMC_clean'] = df['EuropePMC'].str.extract(r'(\d+)', expand=False)
        
        # Create final PMID column with priority
        df['PMID Final'] = (df['PubMed_clean']
                           .combine_first(df['MaxPR_PubMed_clean'])
                           .combine_first(df['EuropePMC_clean']))
        
        print(f"DEBUG: PMIDs consolidated - non-null PMID Final: {df['PMID Final'].notna().sum()}")
        return df
    
    def query_scopus_api(self, scopus_ids, progress_callback=None):
        """Query Scopus API for publication metadata"""
        print(f"DEBUG: Starting API queries for {len(scopus_ids)} Scopus IDs")
        print(f"DEBUG: Sample Scopus IDs: {scopus_ids[:5]}")
        
        results = []
        
        def chunk_list(lst, size):
            for i in range(0, len(lst), size):
                yield lst[i:i + size]
        
        total_batches = len(scopus_ids) // self.batch_size + 1
        
        for batch_index, batch in enumerate(chunk_list(scopus_ids, self.batch_size)):
            if progress_callback:
                progress_callback((batch_index + 1) / total_batches)
            
            for sid in batch:
                url = f"https://api.elsevier.com/content/abstract/eid/{sid}"
                
                for attempt in range(self.max_retries):
                    try:
                        response = requests.get(url, headers=self.headers)
                        
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
                            
                            author_names_initial = [
                                a.get('ce:indexed-name', '').strip()
                                for a in authors if a.get('ce:indexed-name')
                            ]
                            
                            author_ids = [a.get('@auid') for a in authors if a.get('@auid')]
                            author_seq = [a.get('@seq') for a in authors if a.get('@seq')]
                            
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
                            }
                            
                            results.append(result)
                            
                            # Debug the first few results
                            if len(results) <= 3:
                                print(f"DEBUG: Sample API result {len(results)}:")
                                print(f"  Scopus ID: {result['Scopus ID']}")
                                print(f"  Title: {result['Title']}")
                                print(f"  Author IDs: {result['Author IDs']}")
                                print(f"  Document SubType: {result['Document SubType']}")
                                print(f"  Year: {result['Publication Year']}")
                            
                            break
                            
                        elif response.status_code == 429:
                            print(f"Rate limited for {sid}. Retrying...")
                            time.sleep(self.rate_limit_delay)
                        else:
                            print(f"Request failed for {sid}: {response.status_code}")
                            break
                            
                    except Exception as e:
                        print(f"Error processing {sid}: {str(e)}")
                        break
                
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
    
    def flag_author_position(self, claimed_ids, paper_author_ids):
        """Flag author positions (first, last, middle)"""
        if pd.isna(claimed_ids) or pd.isna(paper_author_ids):
            return pd.Series([False, False, False])
        
        # Handle multiple claimed IDs separated by semicolons
        claimed_list = [id.strip() for id in str(claimed_ids).split(';') if id.strip()]
        claimed_set = set(claimed_list)
        
        # Handle paper author IDs separated by semicolons
        paper_list = [a.strip() for a in str(paper_author_ids).split(';') if a.strip()]
        
        if not paper_list or not claimed_set:
            return pd.Series([False, False, False])
        
        # Debug output for first few matches
        if len(claimed_set) > 0 and len(paper_list) > 0:
            intersection = claimed_set.intersection(set(paper_list))
            if intersection:
                print(f"DEBUG: MATCH FOUND!")
                print(f"  Claimed IDs: {claimed_set}")
                print(f"  Paper author IDs: {paper_list}")
                print(f"  Intersection: {intersection}")
        
        # Single author case
        if len(paper_list) == 1:
            first = paper_list[0] in claimed_set
            return pd.Series([first, False, False])
        
        # Multiple authors case
        first = paper_list[0] in claimed_set
        last = paper_list[-1] in claimed_set
        middle = any(a in claimed_set for a in paper_list[1:-1]) if len(paper_list) > 2 else False
        
        return pd.Series([first, last, middle])
    
    def flag_peer_reviewed(self, doc_subtype):
        """Flag peer-reviewed publications"""
        peer_reviewed_types = {'Article', 'Book Chapter', 'Review', 'Short Survey'}
        if pd.isna(doc_subtype):
            return False
        return str(doc_subtype).strip() in peer_reviewed_types
    
    def summarize_pmids(self, df, netid, flag_col):
        """Summarize PMIDs for a given authorship role"""
        subset = df[(df['Username'] == netid) & (df[flag_col])]
        
        pmids = subset['PMID Final']
        pmids_with = pmids[
            pmids.notna() & 
            (pmids.astype(str).str.strip().str.lower() != 'nan') & 
            (pmids.astype(str).str.strip() != '')
        ]
        pmids_without = subset[~subset.index.isin(pmids_with.index)]
        
        return pd.Series({
            f'{flag_col}_PMIDs': ', '.join(pmids_with.astype(str)),
            f'{flag_col}_PMID_Count': len(pmids_with),
            f'{flag_col}_NonPMID_Count': len(pmids_without)
        })
    
    def summarize_any_authorship(self, df, netid):
        """Summarize PMIDs for any author role"""
        subset = df[df['Username'] == netid]
        pmids = subset['PMID Final']
        
        pmids_with = pmids[
            pmids.notna() & 
            (pmids.astype(str).str.strip().str.lower() != 'nan') & 
            (pmids.astype(str).str.strip() != '')
        ]
        pmids_without = subset[~subset.index.isin(pmids_with.index)]
        
        return pd.Series({
            'Any_Position_PMIDs': ', '.join(pmids_with.astype(str)),
            'Any_Position_PMID_Count': len(pmids_with),
            'Any_Position_NonPMID_Count': len(pmids_without)
        })
    
    def process_data(self, df1, df2, start_date, end_date, status_callback=None):
        """Main processing pipeline with extensive debugging"""
        
        def update_status(message):
            if status_callback:
                status_callback(message)
            else:
                print(message)
        
        # Step 1: Consolidate PMIDs
        update_status("🔄 Consolidating PMIDs...")
        df1 = self.consolidate_pmids(df1)
        
        # Step 2: Get unique Scopus IDs and query API
        unique_scopus_ids = df1['Scopus'].dropna().astype(str).unique().tolist()
        
        if not unique_scopus_ids:
            raise ValueError("No Scopus IDs found in the data!")
        
        update_status(f"🔍 Querying Scopus API for {len(unique_scopus_ids)} publications...")
        
        def progress_callback(progress):
            update_status(f"🔍 API Progress: {progress:.1%}")
        
        df_scopus = self.query_scopus_api(unique_scopus_ids, progress_callback)
        
        # Step 3: Add publication dates
        update_status("📅 Processing publication dates...")
        df_scopus['Publication Date'] = df_scopus.apply(
            lambda row: self.combine_date_parts(
                row['Publication Year'], 
                row['Publication Month'], 
                row['Publication Day']
            ), axis=1
        )
        
        print(f"DEBUG: Scopus data shape after date processing: {df_scopus.shape}")
        print(f"DEBUG: Sample publication dates: {df_scopus['Publication Date'].dropna().head().tolist()}")
        
        # Step 4: Merge datasets
        update_status("🔗 Merging datasets...")
        print(f"DEBUG: df1 shape: {df1.shape}, df2 shape: {df2.shape}")
        print(f"DEBUG: df1 merge columns: Username={df1['NetID'].nunique()}, df2 merge columns: NetID={df2['Username'].nunique()}")
        
        df_merged = df2.merge(df1, left_on='Username', right_on='NetID', how='left')
        print(f"DEBUG: After first merge: {df_merged.shape}")
        
        df_merged = df_merged.merge(df_scopus, left_on='Scopus', right_on='Scopus ID', how='left')
        print(f"DEBUG: After second merge: {df_merged.shape}")
        print(f"DEBUG: Non-null Scopus matches: {df_merged['Scopus ID'].notna().sum()}")
        
        # Step 5: Flag author positions
        update_status("👥 Flagging author positions...")
        valid_pub_mask = (
            df_merged['DOI'].notna() |
            df_merged['PMID Final'].notna() |
            df_merged['Title'].notna()
        )
        
        print(f"DEBUG: Valid publications mask: {valid_pub_mask.sum()} out of {len(df_merged)}")
        
        # Initialize all flags to False
        df_merged['Is_First_Author'] = False
        df_merged['Is_Last_Author'] = False
        df_merged['Is_Middle_Author'] = False
        
        # Sample a few rows for detailed debugging
        sample_rows = df_merged[valid_pub_mask].head(5)
        for idx, row in sample_rows.iterrows():
            print(f"\nDEBUG: Processing row {idx}")
            print(f"  Username: {row['Username']}")
            print(f"  ClaimedScopus: {row['ClaimedScopus']}")
            print(f"  Author IDs: {row['Author IDs']}")
            
            first, last, middle = self.flag_author_position(
                row['ClaimedScopus'], 
                row['Author IDs']
            )
            df_merged.at[idx, 'Is_First_Author'] = first
            df_merged.at[idx, 'Is_Last_Author'] = last
            df_merged.at[idx, 'Is_Middle_Author'] = middle
            
            print(f"  Flags: First={first}, Last={last}, Middle={middle}")
        
        # Apply to all valid publications
        for idx, row in df_merged[valid_pub_mask].iterrows():
            first, last, middle = self.flag_author_position(
                row['ClaimedScopus'], 
                row['Author IDs']
            )
            df_merged.at[idx, 'Is_First_Author'] = first
            df_merged.at[idx, 'Is_Last_Author'] = last
            df_merged.at[idx, 'Is_Middle_Author'] = middle
        
        # Step 6: Flag peer-reviewed publications
        update_status("📖 Flagging peer-reviewed publications...")
        df_merged['Is Peer-Reviewed'] = df_merged['Document SubType'].apply(self.flag_peer_reviewed)
        
        print(f"DEBUG: Peer-reviewed publications: {df_merged['Is Peer-Reviewed'].sum()}")
        print(f"DEBUG: Document subtypes found: {df_merged['Document SubType'].value_counts()}")
        
        # Step 7: Filter by date range and peer-review status
        update_status("📊 Filtering publications by date range...")
        df_merged['Publication Date'] = pd.to_datetime(df_merged['Publication Date'], errors='coerce')
        
        print(f"DEBUG: Date range filter: {start_date} to {end_date}")
        print(f"DEBUG: Publications with valid dates: {df_merged['Publication Date'].notna().sum()}")
        
        date_mask = (
            (df_merged['Publication Date'] >= pd.Timestamp(start_date)) &
            (df_merged['Publication Date'] <= pd.Timestamp(end_date))
        )
        print(f"DEBUG: Publications in date range: {date_mask.sum()}")
        
        peer_reviewed_mask = df_merged['Is Peer-Reviewed']
        print(f"DEBUG: Peer-reviewed publications: {peer_reviewed_mask.sum()}")
        
        df_filtered = df_merged[
            peer_reviewed_mask & date_mask
        ].copy()
        
        print(f"DEBUG: Final filtered dataset: {df_filtered.shape}")
        print(f"DEBUG: Authorship flags in filtered data:")
        print(f"  First author: {df_filtered['Is_First_Author'].sum()}")
        print(f"  Last author: {df_filtered['Is_Last_Author'].sum()}")
        print(f"  Middle author: {df_filtered['Is_Middle_Author'].sum()}")
        
        # Step 8: Generate faculty summary (df_8 equivalent)
        update_status("📋 Generating faculty summary...")
        faculty_info = df_merged[['Computed Name Abbreviated', 'Username', 'Position_x', 'Arrive Date', 'Leave Date']].drop_duplicates()
        
        print(f"DEBUG: Faculty count: {len(faculty_info)}")
        
        # Create coauthorship mapping
        if len(df_filtered) > 0:
            scopus_to_netids = df_filtered.groupby('Scopus ID')['Username'].apply(set)
            netid_to_scopus_ids = df_filtered.groupby('Username')['Scopus ID'].apply(set).to_dict()
        else:
            scopus_to_netids = {}
            netid_to_scopus_ids = {}
        
        summary_rows = []
        for _, row in faculty_info.iterrows():
            netid = row['Username']
            person_summary = row.to_dict()
            
            # Debug: Check what we're finding for this person
            person_pubs = df_filtered[df_filtered['Username'] == netid]
            if len(person_pubs) > 0:
                print(f"DEBUG: {netid} has {len(person_pubs)} publications in filtered data")
            
            person_summary.update(self.summarize_pmids(df_filtered, netid, 'Is_First_Author'))
            person_summary.update(self.summarize_pmids(df_filtered, netid, 'Is_Last_Author'))
            person_summary.update(self.summarize_pmids(df_filtered, netid, 'Is_Middle_Author'))
            person_summary.update(self.summarize_any_authorship(df_filtered, netid))
            
            # Check for coauthorship
            coauthored = "No"
            for sid in netid_to_scopus_ids.get(netid, set()):
                if sid in scopus_to_netids and len(scopus_to_netids[sid] - {netid}) > 0:
                    coauthored = "Yes"
                    break
            person_summary["Coauthor with Another Faculty"] = coauthored
            
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
        
        # Final debug output
        any_position_counts = df_faculty_summary['Number of PubMed indexed publications during the past AY (authorship in any position)']
        print(f"DEBUG: Faculty with any position publications: {(any_position_counts > 0).sum()}")
        print(f"DEBUG: Total any position publications: {any_position_counts.sum()}")
        
        # Step 9: Generate publication assignment summary (df_11 equivalent)
        update_status("🎯 Generating publication assignments...")
        
        # Define academic rank order
        rank_order = [
            "Professor", "Research Professor", "Professor, Clinical",
            "Associate Professor", "Research Associate Professor", "Associate Professor, Clinical",
            "Assistant Professor", "Research Assistant Professor", "Assistant Professor, Clinical",
            "Instructor", "Instructor, Clinical", "Lecturer",
            "Adjunct Professor", "Adjunct Associate Professor", "Adjunct Assistant Professor",
            "Adjunct Instructor", "Adjunct Lecturer", "Professor Emeritus"
        ]
        
        position_rank = {title: rank for rank, title in enumerate(rank_order)}
        
        # Filter and assign ranks
        df_assign = df_filtered.copy()
        df_assign["Rank"] = df_assign["Position_x"].map(position_rank)
        df_assign = df_assign.dropna(subset=["DOI"])
        df_assign = df_assign[df_assign["Position_x"].isin(position_rank)]
        
        print(f"DEBUG: Publications for assignment: {len(df_assign)}")
        
        # Group by DOI and assign to highest-ranked person
        def assign_publication(group):
            min_rank = group["Rank"].min()
            top_candidates = group[group["Rank"] == min_rank]
            chosen_one = top_candidates.iloc[0]["Username"]
            return pd.Series({"Assigned To": chosen_one})
        
        if not df_assign.empty:
            df_assignments = df_assign.groupby("DOI").apply(assign_publication).reset_index()
            df_pub_counts = df_assignments["Assigned To"].value_counts().reset_index()
            df_pub_counts.columns = ["Username", "Total Publications"]
        else:
            df_pub_counts = pd.DataFrame(columns=["Username", "Total Publications"])
        
        # Get faculty details and merge
        faculty_details = df_merged[['Username', 'Computed Name Abbreviated', 'Position_x']].drop_duplicates()
        df_pub_summary = faculty_details.merge(df_pub_counts, on="Username", how="left")
        df_pub_summary['Total Publications'] = df_pub_summary['Total Publications'].fillna(0).astype(int)
        df_pub_summary = df_pub_summary[['Username', 'Computed Name Abbreviated', 'Position_x', 'Total Publications']]
        
        print(f"DEBUG: Final publication assignment - faculty with pubs: {(df_pub_summary['Total Publications'] > 0).sum()}")
        
        update_status("✅ Processing completed successfully!")
        
        return df_faculty_summary, df_pub_summary