import streamlit as st
import pandas as pd
import numpy as np
import os
import requests

# --- Utility functions ---

def infer_postgres_type(dtype):
    if pd.api.types.is_integer_dtype(dtype):
        return "bigint"
    if pd.api.types.is_float_dtype(dtype):
        return "double precision"
    if pd.api.types.is_bool_dtype(dtype):
        return "boolean"
    return "text"

def generate_create_table_query(table_name, df):
    column_defs = []
    for col in df.columns:
        safe_col = col.lower().replace(' ', '_')
        pg_type = infer_postgres_type(df[col])
        column_defs.append(f'"{safe_col}" {pg_type}')
    columns_sql = ',\n    '.join(column_defs)
    sql = f'''
    CREATE TABLE IF NOT EXISTS public.{table_name} (
        id serial PRIMARY KEY,
        {columns_sql}
    );
    '''
    return sql

def get_existing_columns(table_name, sql_url, headers):
    get_cols_sql = f"SELECT column_name FROM information_schema.columns WHERE table_name = '{table_name}';"
    resp = requests.post(
        sql_url,
        headers=headers,
        json={"sql": get_cols_sql}
    )
    # Defensive: Check for valid JSON before parsing
    try:
        if resp.ok and resp.content:
            data = resp.json()
            if isinstance(data, list):
                return [row['column_name'] for row in data if 'column_name' in row]
        else:
            st.warning(f"Could not fetch columns, response: {resp.text}")
    except Exception as e:
        st.error(f"Failed to decode columns response: {str(e)}. Raw response: {resp.text}")
    return []


def add_missing_columns(table_name, missing_cols, df, sql_url, headers):
    # For each missing column, add via ALTER TABLE
    for col in missing_cols:
        safe_col = col.lower().replace(' ', '_')
        pg_type = infer_postgres_type(df[col])
        alter_sql = f'ALTER TABLE public.{table_name} ADD COLUMN IF NOT EXISTS "{safe_col}" {pg_type};'
        requests.post(sql_url, headers=headers, json={"sql": alter_sql})

# --- Streamlit App Start ---

st.title("Upload and Save CSV to Supabase (auto schema match)")

uploaded_file = st.file_uploader("Choose a CSV file", type="csv")
if uploaded_file is not None:
    df = pd.read_csv(uploaded_file)
    st.write("Data preview:")
    st.dataframe(df)

    filename, _ = os.path.splitext(uploaded_file.name)
    table_name = filename.lower().replace(' ', '_')
    st.write(f"Destination table: **{table_name}**")

    SUPABASE_URL = st.secrets["SUPABASE_URL"]
    SUPABASE_KEY = st.secrets["SUPABASE_KEY"]
    sql_url = SUPABASE_URL.replace('.co', '.co/rest/v1/rpc/execute_sql')
    headers = {
        "apikey": SUPABASE_KEY,
        "Authorization": f"Bearer {SUPABASE_KEY}",
        "Content-Type": "application/json"
    }

    if st.button(f"Create/Update Table and Insert Data"):
        # 1. Create table if not exists
        create_table_sql = generate_create_table_query(table_name, df)
        resp_create = requests.post(sql_url, headers=headers, json={"sql": create_table_sql})
        if resp_create.ok:
            st.info("Table created/exists. Checking for missing columns...")

            # 2. Get existing columns and add missing ones
            existing_cols = get_existing_columns(table_name, sql_url, headers)
            missing_cols = [col for col in df.columns if col not in existing_cols and col.lower().replace(' ', '_') not in existing_cols]
            if missing_cols:
                add_missing_columns(table_name, missing_cols, df, sql_url, headers)
                st.warning(f"Added {len(missing_cols)} missing columns: " + ", ".join(missing_cols))
            else:
                st.success("All columns present.")

            # 3. Clean and insert the data
            df = df.replace({np.nan: None, np.inf: None, -np.inf: None})
            from supabase import create_client
            supabase = create_client(SUPABASE_URL, SUPABASE_KEY)
            data = df.to_dict(orient="records")
            response = supabase.table(table_name).insert(data).execute()
            if response.status_code == 201:
                st.success(f"Data successfully saved to table {table_name}!")
            else:
                st.error(f"Error saving data: {response.status_code}")
        else:
            st.error(f"Failed to create/alter table: {resp_create.text}")
