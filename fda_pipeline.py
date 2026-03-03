import requests
import pandas as pd
import difflib
from google.oauth2 import service_account
from google.cloud import bigquery

def main():
    print("[START] Launching FDA Shortages ETL Pipeline...")

    # ==========================================
    # 1. EXTRACT: Fetch data from FDA API
    # ==========================================
    print("[EXTRACT] Fetching data from FDA API...")
    all_drugs = []
    limit = 1000
    skip = 0
    is_active = True

    while is_active:
        url = f"https://api.fda.gov/drug/shortages.json?limit={limit}&skip={skip}"
        response = requests.get(url)
        
        if response.status_code == 200:
            data = response.json()
            results = data.get('results', [])
            all_drugs.extend(results)
            print(f"   -> Fetched {len(results)} records. (Total: {len(all_drugs)})")
            
            skip += limit
            
        elif response.status_code == 404:
            print("   -> End of pagination reached.")
            is_active = False
        else:
            print(f"[ERROR] API returned status code {response.status_code}")
            is_active = False

    # ==========================================
    # 2. TRANSFORM: Clean and format data
    # ==========================================
    print("[TRANSFORM] Cleaning and structuring data...")
    df = pd.json_normalize(all_drugs)

    # Filter useful columns to drop unnecessary data
    useful_columns = [
        "generic_name", "company_name", "status", "shortage_reason", 
        "initial_posting_date", "update_date", "therapeutic_category",
        "availability", "discontinued_date", "resolved_note", "dosage_form"
    ]
    columns_to_keep = [col for col in useful_columns if col in df.columns]
    df_clean = df[columns_to_keep].copy()

    # Convert lists to strings to avoid BigQuery insertion errors
    for col in df_clean.columns:
        df_clean[col] = df_clean[col].apply(lambda x: ', '.join(str(i) for i in x) if isinstance(x, list) else x)

    # Remove strict duplicates
    df_clean = df_clean.drop_duplicates()
    print(f"   -> Duplicates removed. {df_clean.shape[0]} unique rows remaining.")

    # Handle missing values
    df_clean.fillna("Not specified", inplace=True)

    # Fuzzy matching for 'availability' column to correct typos
    print("   -> Applying fuzzy matching to correct 'availability' typos...")
    perfect_statuses = ["Available", "Unavailable", "Limited Availability", "Information pending", "Not specified"]

    def correct_availability(word):
        if word == "Not specified" or word == "":
            return "Not specified"
        
        word_lower = str(word).lower()
        statuses_lower = [s.lower() for s in perfect_statuses]
        
        matches = difflib.get_close_matches(word_lower, statuses_lower, n=1, cutoff=0.6)
        
        if matches:
            found_index = statuses_lower.index(matches[0])
            return perfect_statuses[found_index]
        
        return str(word)

    if 'availability' in df_clean.columns:
        df_clean['availability'] = df_clean['availability'].apply(correct_availability)

    # Save a local backup (optional but recommended for auditing)
    local_backup_file = "fda_shortages_clean.csv"
    df_clean.to_csv(local_backup_file, index=False, sep=';', encoding='utf-8')
    print(f"   -> Local backup saved as '{local_backup_file}'.")

    # ==========================================
    # 3. LOAD: Push data to Google BigQuery
    # ==========================================
    print("[LOAD] Pushing data to Google BigQuery...")
    
    # Setup BigQuery connection
    # Make sure this JSON file is in the same directory and named correctly
    KEY_PATH = "google_credentials.json" 
    credentials = service_account.Credentials.from_service_account_file(KEY_PATH)
    client = bigquery.Client(credentials=credentials, project=credentials.project_id)

    # BigQuery table configuration
    # FORMAT: "your_project_id.dataset_name.table_name"
    # REPLACE WITH YOUR ACTUAL PROJECT ID
    table_id = "pharma-supply-watch.pharma_data.current_shortages" 

    # WRITE_TRUNCATE clears old data and replaces it with the new fetch
    job_config = bigquery.LoadJobConfig(
        write_disposition="WRITE_TRUNCATE", 
    )

    try:
        job = client.load_table_from_dataframe(df_clean, table_id, job_config=job_config)
        job.result() # Wait for the upload job to complete
        print(f"[SUCCESS] Loaded {job.output_rows} rows into BigQuery table '{table_id}'.")
    except Exception as e:
        print(f"[ERROR] Failed to upload to BigQuery: {e}")

    print("[END] ETL Pipeline executed successfully.")

if __name__ == "__main__":
    main()