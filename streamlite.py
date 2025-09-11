import streamlit as st
import pandas as pd
import os
from supabase import create_client, Client
from dotenv import load_dotenv
import io
import json
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime

# Page configuration
st.set_page_config(page_title="Metadata-Driven Platform", layout="wide")

# Load environment variables
load_dotenv()

# Supabase configuration
SUPABASE_URL =  st.secrets["SUPABASE_URL"] #os.getenv("VITE_SUPABASE_URL")
SUPABASE_KEY = st.secrets["SUPABASE_KEY"] #os.getenv("SUPABASE_SERVICE_ROLE_KEY")
supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

# Supabase interaction functions
def get_all_tables():
    response = supabase.rpc("get_all_tables").execute()
    if hasattr(response, 'data') and response.data:
        # Only filter tables from the 'public' schema
        tables = [row['table_name'] for row in response.data if row['table_schema'] == 'public']
        return tables
    return []

def get_table_columns(table_name):
    response = supabase.rpc("get_table_columns", {"p_table_name": table_name}).execute()
    if hasattr(response, 'data') and response.data:
        return response.data
    return []

def execute_sql(query):
    response = supabase.rpc("execute_sql", {"query": query}).execute()
    return response.data

def execute_sql_2(query):
    response = supabase.rpc("execute_sql_2", {"query": query}).execute()
    return response.data

def log_data_lineage(source_table, target_table, rows, layer, transformation_query):
    data = {
        "source_table": source_table,
        "target_table": target_table,
        "rows": rows,
        "layer": layer,
        "transformation_query": transformation_query
    }
    response = supabase.table("data_lineage").insert(data).execute()
    return response.data

def save_metadata_mapping(source_table, source_column, target_table, target_column, 
                          transformation_rule, data_type, is_nullable):
    data = {
        "source_table": source_table,
        "source_column": source_column,
        "target_table": target_table,
        "target_column": target_column,
        "transformation_rule": transformation_rule,
        "data_type": data_type,
        "is_nullable": is_nullable
    }
    response = supabase.table("metadata_mappings").insert(data).execute()
    return response.data

def get_metadata_mappings():
    response = supabase.table("metadata_mappings").select("*").execute()
    if hasattr(response, 'data') and response.data:
        return response.data
    return []

# Function to create a new raw table
def create_raw_table(table_name, df):
    # Rename columns according to rules
    df.columns = [col.replace(' ', '_').replace('-', '_').replace('.', '_') for col in df.columns]
    
    # Create SQL script for table creation
    columns = []
    for col in df.columns:
        columns.append(f"{col.upper()} TEXT")
    
    # Add control columns and ID
    columns.insert(0, "ID SERIAL PRIMARY KEY")
    columns.append("CREATED_BY TEXT DEFAULT current_user")
    columns.append("CREATED_AT TIMESTAMPTZ DEFAULT current_timestamp")
    columns.append("MODIFIED_BY TEXT DEFAULT current_user")
    columns.append("MODIFIED_AT TIMESTAMPTZ DEFAULT current_timestamp")
    
    # Create SQL query
    create_table_query = f"CREATE TABLE {table_name} (\n\t" + ",\n\t".join(columns) + "\n);"
    
    # Execute query
    result = execute_sql_2(create_table_query)
    
    # Log in lineage table
    log_data_lineage("upload", table_name, len(df), "raw", create_table_query)
    
    return result

# Function to insert data into table
def insert_data_to_table(table_name, df):
    # Rename columns according to rules
    df.columns = [col.lower()replace(' ', '_').replace('-', '_').replace('.', '_') for col in df.columns]

    # Prepare data for insertion
    records = []
    for _, row in df.iterrows():
        record = {}
        for col in df.columns:
            record[col.upper()] = str(row[col]) if pd.notna(row[col]) else None
        records.append(record)
    
    # Insert data
    response = supabase.table(table_name).insert(records).execute()
    
    # Log in lineage table
    insert_query = f"INSERT INTO {table_name} (...) VALUES (...)"  # Simplified for logging
    log_data_lineage("upload", table_name, len(records), "raw", insert_query)
    
    return response.data

# User Interface
def main():
    # Sidebar for navigation
    st.sidebar.title("Metadata-Driven Platform")
    menu = st.sidebar.radio(
        "Menu",
        ["Data Entry", "Indicator Creation", "Dashboards"]
    )
    
    if menu == "Data Entry":
        data_entry_page()
    elif menu == "Indicator Creation":
        indicator_creation_page()
    elif menu == "Dashboards":
        dashboards_page()

def data_entry_page():
    st.title("Data Entry")
    
    # Get list of existing tables
    tables = get_all_tables()
    raw_tables = [t for t in tables if t.startswith('raw_')]
    
    # Option to select existing table or create new one
    option = st.radio("Choose an option:", ["Insert into existing table", "Create new table"])
    
    if option == "Insert into existing table":
        if not raw_tables:
            st.warning("No raw tables available.")
            return
        
        selected_table = st.selectbox("Select table:", raw_tables)
        
        # File upload
        uploaded_file = st.file_uploader("Choose a CSV or XLSX file", type=["csv", "xlsx"])
        
        if uploaded_file is not None:
            try:
                if uploaded_file.name.endswith('.csv'):
                    df = pd.read_csv(uploaded_file)
                else:
                    df = pd.read_excel(uploaded_file)
                
                st.write("Data preview:")
                st.dataframe(df.head())
                
                if st.button("Insert Data"):
                    with st.spinner("Inserting data..."):
                        result = insert_data_to_table(selected_table, df)
                        st.success(f"Data inserted successfully! {len(df)} records added.")
            except Exception as e:
                st.error(f"Error processing file: {str(e)}")
    
    else:  # Create new table
        new_table_name = st.text_input("New table name (use prefix raw_):")
        
        if not new_table_name.startswith('raw_'):
            st.warning("Table name must start with 'raw_'")
        
        # File upload
        uploaded_file = st.file_uploader("Choose a CSV or XLSX file", type=["csv", "xlsx"])
        
        if uploaded_file is not None and new_table_name.startswith('raw_'):
            try:
                if uploaded_file.name.endswith('.csv'):
                    df = pd.read_csv(uploaded_file)
                else:
                    df = pd.read_excel(uploaded_file)
                
                st.write("Data preview:")
                st.dataframe(df.head())
                
                if st.button("Create Table and Insert Data"):
                    with st.spinner("Creating table and inserting data..."):
                        # Check if table already exists
                        if new_table_name in tables:
                            st.error(f"Table {new_table_name} already exists!")
                        else:
                            # Create table
                            create_result = create_raw_table(new_table_name, df)
                            
                            # Insert data
                            insert_result = insert_data_to_table(new_table_name, df)
                            
                            st.success(f"Table {new_table_name} created and {len(df)} records inserted successfully!")
            except Exception as e:
                st.error(f"Error processing file: {str(e)}")

def indicator_creation_page():
    st.title("Indicator Creation")
    
    # Get list of tables
    tables = get_all_tables()
    
    # Interface for indicator creation
    col1, col2 = st.columns(2)
    
    with col1:
        st.subheader("Indicator Configuration")
        indicator_name = st.text_input("Indicator Name:")
        
        # Select source tables
        source_tables = st.multiselect("Select source tables:", tables)
        
        # Show columns of selected tables
        all_columns = {}
        for table in source_tables:
            columns = get_table_columns(table)
            all_columns[table] = [col['column_name'] for col in columns]
            
        # Select columns for indicator
        selected_columns = {}
        for table in source_tables:
            if table in all_columns:
                st.write(f"Columns in table {table}:")
                selected_columns[table] = st.multiselect(
                    f"Select columns from {table}:",
                    all_columns[table],
                    key=f"cols_{table}"
                )
    
    with col2:
        st.subheader("Join and Filter Configuration")
        
        # Configure joins if more than one table
        joins = []
        if len(source_tables) > 1:
            st.write("Join Configuration:")
            
            for i in range(len(source_tables) - 1):
                st.write(f"Join {i+1}:")
                join_type = st.selectbox(
                    "Join Type:",
                    ["INNER JOIN", "LEFT JOIN", "RIGHT JOIN", "FULL JOIN"],
                    key=f"join_type_{i}"
                )
                
                left_table = st.selectbox(
                    "Left Table:",
                    source_tables,
                    key=f"left_table_{i}"
                )
                
                right_table = st.selectbox(
                    "Right Table:",
                    source_tables,
                    key=f"right_table_{i}"
                )
                
                if left_table in all_columns and right_table in all_columns:
                    left_column = st.selectbox(
                        "Left Table Column:",
                        all_columns[left_table],
                        key=f"left_col_{i}"
                    )
                    
                    right_column = st.selectbox(
                        "Right Table Column:",
                        all_columns[right_table],
                        key=f"right_col_{i}"
                    )
                    
                    joins.append({
                        "type": join_type,
                        "left_table": left_table,
                        "left_column": left_column,
                        "right_table": right_table,
                        "right_column": right_column
                    })
        
        # Filter configuration
        st.write("Filter Configuration:")
        add_filter = st.checkbox("Add filter")
        
        filters = []
        if add_filter:
            filter_table = st.selectbox("Filter Table:", source_tables)
            
            if filter_table in all_columns:
                filter_column = st.selectbox("Filter Column:", all_columns[filter_table])
                filter_operator = st.selectbox("Operator:", ["=", ">", "<", ">=", "<=", "LIKE", "IN", "NOT IN"])
                filter_value = st.text_input("Value:")
                
                filters.append({
                    "table": filter_table,
                    "column": filter_column,
                    "operator": filter_operator,
                    "value": filter_value
                })
        
        # Order configuration
        st.write("Order Configuration:")
        add_order = st.checkbox("Add ordering")
        
        orders = []
        if add_order:
            order_table = st.selectbox("Order Table:", source_tables)
            
            if order_table in all_columns:
                order_column = st.selectbox("Order Column:", all_columns[order_table])
                order_direction = st.selectbox("Direction:", ["ASC", "DESC"])
                
                orders.append({
                    "table": order_table,
                    "column": order_column,
                    "direction": order_direction
                })
    
    # SQL query preview
    st.subheader("SQL Query Preview")
    
    # Build the SQL query
    query = ""
    if source_tables and all(table in all_columns for table in source_tables):
        # Build SELECT clause
        select_columns = []
        for table in source_tables:
            for col in selected_columns.get(table, []):
                select_columns.append(f"{table}.{col}")
        
        if select_columns:
            query = "SELECT " + ", ".join(select_columns) + "\n"
            
            # FROM clause
            query += f"FROM {source_tables[0]}\n"
            
            # Add JOINs
            for join in joins:
                query += f"{join['type']} {join['right_table']} ON {join['left_table']}.{join['left_column']} = {join['right_table']}.{join['right_column']}\n"
            
            # Add filters (WHERE)
            if filters:
                query += "WHERE "
                filter_conditions = []
                for f in filters:
                    if f["operator"] in ["IN", "NOT IN"]:
                        filter_conditions.append(f"{f['table']}.{f['column']} {f['operator']} ({f['value']})")
                    elif f["operator"] == "LIKE":
                        filter_conditions.append(f"{f['table']}.{f['column']} {f['operator']} '%{f['value']}%'")
                    else:
                        filter_conditions.append(f"{f['table']}.{f['column']} {f['operator']} '{f['value']}'")
                query += " AND ".join(filter_conditions) + "\n"
            
            # Add ordering (ORDER BY)
            if orders:
                query += "ORDER BY "
                order_conditions = []
                for o in orders:
                    order_conditions.append(f"{o['table']}.{o['column']} {o['direction']}")
                query += ", ".join(order_conditions)
    
    # Show query
    st.code(query)
    
    # Button to test query
    if query and st.button("Test Query"):
        try:
            with st.spinner("Executing query..."):
                result = execute_sql_2(query)
                st.write("Query Result:")
                # Convert result to DataFrame
                if result:
                    df_result = pd.DataFrame(result)
                    st.dataframe(df_result)
                else:
                    st.info("Query returned no results.")
        except Exception as e:
            st.error(f"Error executing query: {str(e)}")
    
    # Button to save indicator
    if query and indicator_name and st.button("Save Indicator"):
        try:
            # Prepare data to save
            for table in source_tables:
                for col in selected_columns.get(table, []):
                    # For each selected column, create a record in the metadata_mappings table
                    save_metadata_mapping(
                        source_table=table,
                        source_column=col,
                        target_table="indicator_" + indicator_name.lower().replace(" ", "_"),
                        target_column=col,
                        transformation_rule=query,
                        data_type="TEXT",  # Simplified, can be improved
                        is_nullable=True
                    )
            
            # Log in lineage table
            source_tables_str = ", ".join(source_tables)
            log_data_lineage(
                source_table=source_tables_str,
                target_table="indicator_" + indicator_name.lower().replace(" ", "_"),
                rows=0,  # Exact number not available here
                layer="indicator",
                transformation_query=query
            )
            
            st.success(f"Indicator '{indicator_name}' saved successfully!")
        except Exception as e:
            st.error(f"Error saving indicator: {str(e)}")

def dashboards_page():
    st.title("Dashboards")
    
    # Get available indicators
    mappings = get_metadata_mappings()
    
    if not mappings:
        st.warning("No indicators available. Create indicators on the 'Indicator Creation' page.")
        return
    
    # Group by target_table (indicators)
    indicators = {}
    for mapping in mappings:
        target = mapping.get('target_table', '')
        if target.startswith('indicator_'):
            if target not in indicators:
                indicators[target] = []
            indicators[target].append(mapping)
    
    # Select indicator to view
    indicator_names = list(indicators.keys())
    selected_indicator = st.selectbox("Select an indicator:", indicator_names)
    
    if selected_indicator:
        # Get indicator query
        indicator_data = indicators[selected_indicator]
        query = indicator_data[0].get('transformation_rule', '')
        
        st.subheader(f"Indicator Visualization: {selected_indicator.replace('indicator_', '').replace('_', ' ').title()}")
        
        # Execute query to get indicator data
        try:
            with st.spinner("Loading indicator data..."):
                result = execute_sql_2(query)
                
                if result:
                    # Convert result to DataFrame
                    df_result = pd.DataFrame(result)
                    
                    # Show data in table
                    st.subheader("Indicator Data")
                    st.dataframe(df_result)
                    
                    # Visualizations
                    st.subheader("Visualizations")
                    
                    # Automatically determine visualization type based on data
                    viz_type = st.selectbox(
                        "Visualization Type:",
                        ["Table", "Bar Chart", "Line Chart", "Pie Chart", "Heatmap"]
                    )
                    
                    if viz_type == "Table":
                        st.dataframe(df_result)
                    
                    elif viz_type == "Bar Chart":
                        col1, col2 = st.columns(2)
                        with col1:
                            x_axis = st.selectbox("X Axis:", df_result.columns)
                        with col2:
                            y_axis = st.selectbox("Y Axis:", df_result.columns, index=min(1, len(df_result.columns)-1))
                        fig = px.bar(df_result, x=x_axis, y=y_axis, title=f"{selected_indicator.replace('indicator_', '').replace('_', ' ').title()}")
                        st.plotly_chart(fig, use_container_width=True)
                    
                    elif viz_type == "Line Chart":
                        col1, col2 = st.columns(2)
                        with col1:
                            x_axis = st.selectbox("X Axis (Time):", df_result.columns)
                        with col2:
                            y_axis = st.selectbox("Y Axis (Value):", df_result.columns, index=min(1, len(df_result.columns)-1))
                        fig = px.line(df_result, x=x_axis, y=y_axis, title=f"{selected_indicator.replace('indicator_', '').replace('_', ' ').title()}")
                        st.plotly_chart(fig, use_container_width=True)
                    
                    elif viz_type == "Pie Chart":
                        col1, col2 = st.columns(2)
                        with col1:
                            names = st.selectbox("Names:", df_result.columns)
                        with col2:
                            values = st.selectbox("Values:", df_result.columns, index=min(1, len(df_result.columns)-1))
                        fig = px.pie(df_result, names=names, values=values, title=f"{selected_indicator.replace('indicator_', '').replace('_', ' ').title()}")
                        st.plotly_chart(fig, use_container_width=True)
                    
                    elif viz_type == "Heatmap":
                        numeric_cols = df_result.select_dtypes(include=['number']).columns.tolist()
                        if len(numeric_cols) >= 2:
                            fig = px.imshow(
                                df_result[numeric_cols].corr(),
                                title=f"Correlation Heatmap - {selected_indicator.replace('indicator_', '').replace('_', ' ').title()}"
                            )
                            st.plotly_chart(fig, use_container_width=True)
                        else:
                            st.warning("Not enough numeric columns to create a heatmap.")
                    
                    # Option to export data
                    csv = df_result.to_csv(index=False).encode('utf-8')
                    st.download_button(
                        "Export data as CSV",
                        data=csv,
                        file_name=f"{selected_indicator.replace('indicator_', '')}.csv",
                        mime="text/csv",
                    )
                else:
                    st.info("Indicator returned no results.")
        except Exception as e:
            st.error(f"Error loading indicator data: {str(e)}")

# Run the app
if __name__ == "__main__":
    main()
