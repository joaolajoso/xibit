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

# Configuração da página
st.set_page_config(page_title="Plataforma Metadata-Driven", layout="wide")

# Carregamento das variáveis de ambiente
load_dotenv()

# Configurações do Supabase
SUPABASE_URL =  st.secrets["SUPABASE_URL"] #os.getenv("VITE_SUPABASE_URL")
SUPABASE_KEY = st.secrets["SUPABASE_KEY"] #os.getenv("SUPABASE_SERVICE_ROLE_KEY")

supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

# Funções para interagir com o Supabase
def get_all_tables():
    response = supabase.rpc("get_all_tables").execute()
    if hasattr(response, 'data') and response.data:
        # Filtra apenas tabelas do schema 'public'
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

# Função para criar nova tabela raw
def create_raw_table(table_name, df):
    # Renomear colunas conforme regras
    df.columns = [col.replace(' ', '_').replace('-', '_').replace('.', '_') for col in df.columns]
    
    # Criar script SQL para criação da tabela
    columns = []
    for col in df.columns:
        columns.append(f"{col.upper()} TEXT")
    
    # Adicionar colunas de controle e ID
    columns.insert(0, "ID SERIAL PRIMARY KEY")
    columns.append("DS_CRTD_BY TEXT DEFAULT current_user")
    columns.append("DT_CRTD TIMESTAMPTZ DEFAULT current_timestamp")
    columns.append("DS_MDFD_BY TEXT DEFAULT current_user")
    columns.append("DT_MDFD TIMESTAMPTZ DEFAULT current_timestamp")
    
    # Criar query SQL
    create_table_query = f"CREATE TABLE {table_name} (\n\t" + ",\n\t".join(columns) + "\n);"
    
    # Executar query
    result = execute_sql(create_table_query)
    
    # Registrar na tabela de linhagem
    log_data_lineage("upload", table_name, len(df), "raw", create_table_query)
    
    return result

# Função para inserir dados na tabela
def insert_data_to_table(table_name, df):
    # Renomear colunas conforme regras
    df.columns = [col.replace(' ', '_').replace('-', '_').replace('.', '_') for col in df.columns]
    
    # Preparar dados para inserção
    records = []
    for _, row in df.iterrows():
        record = {}
        for col in df.columns:
            record[col.upper()] = str(row[col]) if pd.notna(row[col]) else None
        records.append(record)
    
    # Inserir dados
    response = supabase.table(table_name).insert(records).execute()
    
    # Registrar na tabela de linhagem
    insert_query = f"INSERT INTO {table_name} (...) VALUES (...)"  # Simplificado para o registro
    log_data_lineage("upload", table_name, len(records), "raw", insert_query)
    
    return response.data

# Interface do usuário
def main():
    # Sidebar para navegação
    st.sidebar.title("Plataforma Metadata-Driven")
    menu = st.sidebar.radio(
        "Menu",
        ["Entrada de Dados", "Criação de Indicadores", "Dashboards"]
    )
    
    if menu == "Entrada de Dados":
        entrada_dados_page()
    elif menu == "Criação de Indicadores":
        criacao_indicadores_page()
    elif menu == "Dashboards":
        dashboards_page()

def entrada_dados_page():
    st.title("Entrada de Dados")
    
    # Obter lista de tabelas existentes
    tables = get_all_tables()
    raw_tables = [t for t in tables if t.startswith('raw_')]
    
    # Opção para selecionar tabela existente ou criar nova
    option = st.radio("Selecione uma opção:", ["Inserir em tabela existente", "Criar nova tabela"])
    
    if option == "Inserir em tabela existente":
        if not raw_tables:
            st.warning("Não existem tabelas raw disponíveis.")
            return
        
        selected_table = st.selectbox("Selecione a tabela:", raw_tables)
        
        # Upload de arquivo
        uploaded_file = st.file_uploader("Escolha um arquivo CSV ou XLSX", type=["csv", "xlsx"])
        
        if uploaded_file is not None:
            try:
                if uploaded_file.name.endswith('.csv'):
                    df = pd.read_csv(uploaded_file)
                else:
                    df = pd.read_excel(uploaded_file)
                
                st.write("Preview dos dados:")
                st.dataframe(df.head())
                
                if st.button("Inserir Dados"):
                    with st.spinner("Inserindo dados..."):
                        result = insert_data_to_table(selected_table, df)
                        st.success(f"Dados inseridos com sucesso! {len(df)} registros adicionados.")
            except Exception as e:
                st.error(f"Erro ao processar o arquivo: {str(e)}")
    
    else:  # Criar nova tabela
        new_table_name = st.text_input("Nome da nova tabela (use prefixo raw_):")
        
        if not new_table_name.startswith('raw_'):
            st.warning("O nome da tabela deve começar com 'raw_'")
        
        # Upload de arquivo
        uploaded_file = st.file_uploader("Escolha um arquivo CSV ou XLSX", type=["csv", "xlsx"])
        
        if uploaded_file is not None and new_table_name.startswith('raw_'):
            try:
                if uploaded_file.name.endswith('.csv'):
                    df = pd.read_csv(uploaded_file)
                else:
                    df = pd.read_excel(uploaded_file)
                
                st.write("Preview dos dados:")
                st.dataframe(df.head())
                
                if st.button("Criar Tabela e Inserir Dados"):
                    with st.spinner("Criando tabela e inserindo dados..."):
                        # Verificar se a tabela já existe
                        if new_table_name in tables:
                            st.error(f"A tabela {new_table_name} já existe!")
                        else:
                            # Criar tabela
                            create_result = create_raw_table(new_table_name, df)
                            
                            # Inserir dados
                            insert_result = insert_data_to_table(new_table_name, df)
                            
                            st.success(f"Tabela {new_table_name} criada e {len(df)} registros inseridos com sucesso!")
            except Exception as e:
                st.error(f"Erro ao processar o arquivo: {str(e)}")

def criacao_indicadores_page():
    st.title("Criação de Indicadores")
    
    # Obter lista de tabelas
    tables = get_all_tables()
    
    # Interface para construção de indicadores
    col1, col2 = st.columns(2)
    
    with col1:
        st.subheader("Configuração do Indicador")
        indicator_name = st.text_input("Nome do Indicador:")
        
        # Seleção de tabelas fonte
        source_tables = st.multiselect("Selecione as tabelas fonte:", tables)
        
        # Mostrar colunas das tabelas selecionadas
        all_columns = {}
        for table in source_tables:
            columns = get_table_columns(table)
            all_columns[table] = [col['column_name'] for col in columns]
            
        # Seleção de colunas para o indicador
        selected_columns = {}
        for table in source_tables:
            if table in all_columns:
                st.write(f"Colunas da tabela {table}:")
                selected_columns[table] = st.multiselect(
                    f"Selecione colunas de {table}:",
                    all_columns[table],
                    key=f"cols_{table}"
                )
    
    with col2:
        st.subheader("Configuração de Joins e Filtros")
        
        # Configuração de joins se houver mais de uma tabela
        joins = []
        if len(source_tables) > 1:
            st.write("Configuração de Joins:")
            
            for i in range(len(source_tables) - 1):
                st.write(f"Join {i+1}:")
                join_type = st.selectbox(
                    "Tipo de Join:",
                    ["INNER JOIN", "LEFT JOIN", "RIGHT JOIN", "FULL JOIN"],
                    key=f"join_type_{i}"
                )
                
                left_table = st.selectbox(
                    "Tabela Esquerda:",
                    source_tables,
                    key=f"left_table_{i}"
                )
                
                right_table = st.selectbox(
                    "Tabela Direita:",
                    source_tables,
                    key=f"right_table_{i}"
                )
                
                if left_table in all_columns and right_table in all_columns:
                    left_column = st.selectbox(
                        "Coluna da Tabela Esquerda:",
                        all_columns[left_table],
                        key=f"left_col_{i}"
                    )
                    
                    right_column = st.selectbox(
                        "Coluna da Tabela Direita:",
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
        
        # Configuração de filtros
        st.write("Configuração de Filtros:")
        add_filter = st.checkbox("Adicionar filtro")
        
        filters = []
        if add_filter:
            filter_table = st.selectbox("Tabela para filtro:", source_tables)
            
            if filter_table in all_columns:
                filter_column = st.selectbox("Coluna para filtro:", all_columns[filter_table])
                filter_operator = st.selectbox("Operador:", ["=", ">", "<", ">=", "<=", "LIKE", "IN", "NOT IN"])
                filter_value = st.text_input("Valor:")
                
                filters.append({
                    "table": filter_table,
                    "column": filter_column,
                    "operator": filter_operator,
                    "value": filter_value
                })
        
        # Configuração de ordenação
        st.write("Configuração de Ordenação:")
        add_order = st.checkbox("Adicionar ordenação")
        
        orders = []
        if add_order:
            order_table = st.selectbox("Tabela para ordenação:", source_tables)
            
            if order_table in all_columns:
                order_column = st.selectbox("Coluna para ordenação:", all_columns[order_table])
                order_direction = st.selectbox("Direção:", ["ASC", "DESC"])
                
                orders.append({
                    "table": order_table,
                    "column": order_column,
                    "direction": order_direction
                })
    
    # Previsualização da query SQL
    st.subheader("Previsualização da Query SQL")
    
    # Construir a query SQL
    query = ""
    if source_tables and all(table in all_columns for table in source_tables):
        # Construir a cláusula SELECT
        select_columns = []
        for table in source_tables:
            for col in selected_columns.get(table, []):
                select_columns.append(f"{table}.{col}")
        
        if select_columns:
            query = "SELECT " + ", ".join(select_columns) + "\n"
            
            # Construir a cláusula FROM
            query += f"FROM {source_tables[0]}\n"
            
            # Adicionar JOINs
            for join in joins:
                query += f"{join['type']} {join['right_table']} ON {join['left_table']}.{join['left_column']} = {join['right_table']}.{join['right_column']}\n"
            
            # Adicionar filtros (WHERE)
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
            
            # Adicionar ordenação (ORDER BY)
            if orders:
                query += "ORDER BY "
                order_conditions = []
                for o in orders:
                    order_conditions.append(f"{o['table']}.{o['column']} {o['direction']}")
                query += ", ".join(order_conditions)
    
    # Mostrar a query
    st.code(query)
    
    # Botão para testar a query
    if query and st.button("Testar Query"):
        try:
            with st.spinner("Executando query..."):
                result = execute_sql_2(query)
                st.write("Resultado da Query:")
                # Converter o resultado para DataFrame
                if result:
                    df_result = pd.DataFrame(result)
                    st.dataframe(df_result)
                else:
                    st.info("A query não retornou resultados.")
        except Exception as e:
            st.error(f"Erro ao executar a query: {str(e)}")
    
    # Botão para salvar o indicador
    if query and indicator_name and st.button("Salvar Indicador"):
        try:
            # Preparar dados para salvar
            for table in source_tables:
                for col in selected_columns.get(table, []):
                    # Para cada coluna selecionada, criar um registro na tabela metadata_mappings
                    save_metadata_mapping(
                        source_table=table,
                        source_column=col,
                        target_table="indicador_" + indicator_name.lower().replace(" ", "_"),
                        target_column=col,
                        transformation_rule=query,
                        data_type="TEXT",  # Simplificado, pode ser melhorado
                        is_nullable=True
                    )
            
            # Registrar na tabela de linhagem
            source_tables_str = ", ".join(source_tables)
            log_data_lineage(
                source_table=source_tables_str,
                target_table="indicador_" + indicator_name.lower().replace(" ", "_"),
                rows=0,  # Não temos o número exato aqui
                layer="indicador",
                transformation_query=query
            )
            
            st.success(f"Indicador '{indicator_name}' salvo com sucesso!")
        except Exception as e:
            st.error(f"Erro ao salvar o indicador: {str(e)}")

def dashboards_page():
    st.title("Dashboards")
    
    # Obter indicadores disponíveis
    mappings = get_metadata_mappings()
    
    if not mappings:
        st.warning("Não há indicadores disponíveis. Crie indicadores na página 'Criação de Indicadores'.")
        return
    
    # Agrupar por target_table (indicadores)
    indicators = {}
    for mapping in mappings:
        target = mapping.get('target_table', '')
        if target.startswith('indicador_'):
            if target not in indicators:
                indicators[target] = []
            indicators[target].append(mapping)
    
    # Selecionar indicador para visualizar
    indicator_names = list(indicators.keys())
    selected_indicator = st.selectbox("Selecione um indicador:", indicator_names)
    
    if selected_indicator:
        # Obter a query do indicador
        indicator_data = indicators[selected_indicator]
        query = indicator_data[0].get('transformation_rule', '')
        
        st.subheader(f"Visualização do Indicador: {selected_indicator.replace('indicador_', '').replace('_', ' ').title()}")
        
        # Executar a query para obter os dados do indicador
        try:
            with st.spinner("Carregando dados do indicador..."):
                result = execute_sql_2(query)
                
                if result:
                    # Converter o resultado para DataFrame
                    df_result = pd.DataFrame(result)
                    
                    # Mostrar os dados em uma tabela
                    st.subheader("Dados do Indicador")
                    st.dataframe(df_result)
                    
                    # Visualizações
                    st.subheader("Visualizações")
                    
                    # Determinar automaticamente o tipo de visualização com base nos dados
                    viz_type = st.selectbox(
                        "Tipo de Visualização:",
                        ["Tabela", "Gráfico de Barras", "Gráfico de Linhas", "Gráfico de Pizza", "Mapa de Calor"]
                    )
                    
                    if viz_type == "Tabela":
                        st.dataframe(df_result)
                    
                    elif viz_type == "Gráfico de Barras":
                        # Configuração do gráfico de barras
                        col1, col2 = st.columns(2)
                        
                        with col1:
                            x_axis = st.selectbox("Eixo X:", df_result.columns)
                        
                        with col2:
                            y_axis = st.selectbox("Eixo Y:", df_result.columns, index=min(1, len(df_result.columns)-1))
                        
                        # Criar gráfico de barras
                        fig = px.bar(df_result, x=x_axis, y=y_axis, title=f"{selected_indicator.replace('indicador_', '').replace('_', ' ').title()}")
                        st.plotly_chart(fig, use_container_width=True)
                    
                    elif viz_type == "Gráfico de Linhas":
                        # Configuração do gráfico de linhas
                        col1, col2 = st.columns(2)
                        
                        with col1:
                            x_axis = st.selectbox("Eixo X (Temporal):", df_result.columns)
                        
                        with col2:
                            y_axis = st.selectbox("Eixo Y (Valor):", df_result.columns, index=min(1, len(df_result.columns)-1))
                        
                        # Criar gráfico de linhas
                        fig = px.line(df_result, x=x_axis, y=y_axis, title=f"{selected_indicator.replace('indicador_', '').replace('_', ' ').title()}")
                        st.plotly_chart(fig, use_container_width=True)
                    
                    elif viz_type == "Gráfico de Pizza":
                        # Configuração do gráfico de pizza
                        col1, col2 = st.columns(2)
                        
                        with col1:
                            names = st.selectbox("Nomes:", df_result.columns)
                        
                        with col2:
                            values = st.selectbox("Valores:", df_result.columns, index=min(1, len(df_result.columns)-1))
                        
                        # Criar gráfico de pizza
                        fig = px.pie(df_result, names=names, values=values, title=f"{selected_indicator.replace('indicador_', '').replace('_', ' ').title()}")
                        st.plotly_chart(fig, use_container_width=True)
                    
                    elif viz_type == "Mapa de Calor":
                        # Verificar se há dados numéricos suficientes
                        numeric_cols = df_result.select_dtypes(include=['number']).columns.tolist()
                        
                        if len(numeric_cols) >= 2:
                            # Criar mapa de calor
                            fig = px.imshow(
                                df_result[numeric_cols].corr(),
                                title=f"Mapa de Calor de Correlação - {selected_indicator.replace('indicador_', '').replace('_', ' ').title()}"
                            )
                            st.plotly_chart(fig, use_container_width=True)
                        else:
                            st.warning("Não há colunas numéricas suficientes para criar um mapa de calor.")
                    
                    # Opção para exportar dados
                    csv = df_result.to_csv(index=False).encode('utf-8')
                    st.download_button(
                        "Exportar dados como CSV",
                        data=csv,
                        file_name=f"{selected_indicator.replace('indicador_', '')}.csv",
                        mime="text/csv",
                    )
                else:
                    st.info("O indicador não retornou resultados.")
        except Exception as e:
            st.error(f"Erro ao carregar dados do indicador: {str(e)}")

# Executar a aplicação
if __name__ == "__main__":
    main()
