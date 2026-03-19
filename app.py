import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
from core.processing import *
from core.kpis import *
from core.config import *
from datetime import datetime

# Configuração da Página
st.set_page_config(page_title="Dashboard Outbound MEO", layout="wide")

bg_main = "#121212"
bg_metric = "#1e2130"
text_col = "#FFFFFF"
accent = "#00E5FF"
border_c = "#30363d"
plotly_template = "plotly_dark"

# Estilos Customizados
st.markdown(f"""
<style>
    .main {{ background-color: {bg_main}; color: {text_col}; }}
    .stApp {{ background-color: {bg_main}; color: {text_col}; }}
    h1, h2, h3, h4, h5, h6 {{ color: {accent} !important; }}
    div[data-testid="stDataFrame"] td {{ text-align: center !important; }}
    div[data-testid="stDataFrame"] th {{ text-align: center !important; }}
    div[data-testid="stMetric"] {{ background-color: {bg_metric}; padding: 15px; border-radius: 12px; border: 1px solid {border_c}; box-shadow: 0 4px 6px rgba(0,0,0,0.1); }}
    div[data-testid="stMetricValue"] {{ color: {text_col} !important; font-weight: 700; }}
</style>
""", unsafe_allow_html=True)

# Sidebar - Filtros
st.sidebar.title("Filtros de Análise 📊")

@st.cache_data(show_spinner="A ler milhões de linhas... Isto só demora na primeira vez! ⏳")
def get_cloud_data():
    base_path = os.path.join(os.path.dirname(__file__), "data")
    df_tel = load_telefonia(os.path.join(base_path, "telefonia"))
    df_logs = load_logs(os.path.join(base_path, "logs"))
    df_doc = load_doc(os.path.join(base_path, "doc"))
    df_cad = get_cadastro("https://docs.google.com/spreadsheets/d/e/2PACX-1vR4TcU_DfddmbjJrNfElt8w-9rXlDdxyA5oOkNwZm-GZ_0Rr7vQXG2kCGoJGxQgBnifpa5MBSJsY2j1/pub?output=csv")
    return df_tel, df_logs, df_doc, df_cad

try:
    df_tel, df_logs, df_doc, df_cad = get_cloud_data()
    if df_tel.empty:
        st.warning("⚠️ Pastas de dados vazias! Coloque os ficheiros em `data/`.")
        st.stop()
except Exception as e:
    st.error(f"❌ Erro de Configuração: {e}")
    st.stop()

# --- Filtros Dinâmicos ---
campanha_selecionada = st.sidebar.selectbox("Campanha 🎯", ["Alto Valor", "Rede Móvel", "Fibra", "Móvel Inativos"])

st.sidebar.markdown("### Período 📅")
default_date = datetime.now().date()
date_range = st.sidebar.date_input(
    "Selecione o intervalo:",
    value=(default_date, default_date),
    format="DD/MM/YYYY"
)

# Lógica de Filtro por Calendário
if isinstance(date_range, tuple) and len(date_range) == 2:
    start_date, end_date = date_range
    df_filtered_tel = df_tel[(df_tel['Data_Hora_DT'].dt.date >= start_date) & (df_tel['Data_Hora_DT'].dt.date <= end_date)]
else:
    start_date = date_range[0] if isinstance(date_range, (list, tuple)) else date_range
    df_filtered_tel = df_tel[df_tel['Data_Hora_DT'].dt.date == start_date]

# Filtrar pela Campanha Mapeada
df_filtered_tel = df_filtered_tel[df_filtered_tel['Campanha_Mapeada'] == campanha_selecionada]

# Cabeçalho Principal
st.title(f"Dashboard Outbound MEO - {campanha_selecionada}")

# Cruzamento com Cadastro
main_df = pd.merge(df_filtered_tel, df_cad[['Agente_NIF', 'Team Leader', 'Listagem Assistentes']], on='Agente_NIF', how='inner')

if main_df.empty:
    st.warning("⚠️ Nenhum dado encontrado para os filtros selecionados.")
    st.stop()

# --- Cálculos de KPIs ---
kpi_table = main_df.groupby(['Team Leader', 'Listagem Assistentes', 'Agente_NIF']).agg({
    COLUMNS_TELEFONIA['talk_time']: 'sum',
    'Agente_NIF': 'count'
}).rename(columns={COLUMNS_TELEFONIA['talk_time']: 'Talk_Time_Total', 'Agente_NIF': 'Vol_Atendidas'}).reset_index()

# Filtro de DOC e Logs pelo período
if not df_doc.empty and 'Data_Hora_DT' in df_doc.columns:
    df_filtered_doc = df_doc[(df_doc['Data_Hora_DT'].dt.date >= df_filtered_tel['Data_Hora_DT'].dt.date.min()) & 
                            (df_doc['Data_Hora_DT'].dt.date <= df_filtered_tel['Data_Hora_DT'].dt.date.max())]
else:
    df_filtered_doc = pd.DataFrame(columns=['Agente_NIF', 'Data_Hora_DT'])

sales_count = get_sales_per_agent(df_filtered_tel, df_filtered_doc)
kpi_table['Vendas'] = kpi_table['Agente_NIF'].map(sales_count).fillna(0)

# Outros KPIs
df_filtered_logs = df_logs[(df_logs['Data_Hora_DT'].dt.date >= df_filtered_tel['Data_Hora_DT'].dt.date.min()) & 
                           (df_logs['Data_Hora_DT'].dt.date <= df_filtered_tel['Data_Hora_DT'].dt.date.max())]
login_times_map = calculate_login_time(df_filtered_logs, campanha_selecionada)
kpi_table['Login_Seconds'] = kpi_table['Agente_NIF'].map(login_times_map).fillna(0)

kpi_table['TMC_Sec'] = kpi_table['Talk_Time_Total'] / kpi_table['Vol_Atendidas']
kpi_table['HR %'] = (kpi_table['Vendas'] / kpi_table['Vol_Atendidas'] * 100).fillna(0)

# --- Exibição da Tabela Principal ---
st.subheader("📊 Performance Agentes e Team Leaders")

display_table = kpi_table.copy()
display_table['Total de Conversação'] = display_table['Talk_Time_Total'].apply(format_hms)
display_table['TMC'] = display_table['TMC_Sec'].apply(format_hms)
display_table['Tempo Login'] = display_table['Login_Seconds'].apply(format_hms)

# Renomear para o layout solicitado
display_table = display_table.rename(columns={
    'Listagem Assistentes': 'Agentes',
    'Vol_Atendidas': 'Chamadas Atendidas'
})

cols_to_show = ['Team Leader', 'Agentes', 'Chamadas Atendidas', 'Vendas', 'HR %', 'Total de Conversação', 'TMC', 'Tempo Login']

st.dataframe(
    display_table[cols_to_show].sort_values('Vendas', ascending=False),
    use_container_width=True,
    hide_index=True,
    column_config={
        "Agentes": st.column_config.TextColumn("Agentes"),
        "Chamadas Atendidas": st.column_config.NumberColumn("Chamadas Atendidas", format="%d"),
        "Vendas": st.column_config.NumberColumn("Vendas", format="%d"),
        "HR %": st.column_config.ProgressColumn("Hit Ratio", format="%.2f%%", min_value=0, max_value=20),
        "Total de Conversação": st.column_config.TextColumn("Total de Conversação"),
        "TMC": st.column_config.TextColumn("TMC"),
        "Tempo Login": st.column_config.TextColumn("Tempo Login"),
    }
)

# --- Detalhe do Agente ---
agente_selecionado = st.selectbox("🎯 Escolha um Agente para Analisar:", 
                                  options=["Selecione..."] + sorted(list(kpi_table['Listagem Assistentes'].unique())))

if agente_selecionado != "Selecione...":
    st.divider()
    st.subheader(f"🔍 Análise Detalhada: {agente_selecionado}")
    ag_data = kpi_table[kpi_table['Listagem Assistentes'] == agente_selecionado].iloc[0]
    nif_ag = ag_data['Agente_NIF']
    
    # Métricas
    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Atendidas", int(ag_data['Vol_Atendidas']))
    c2.metric("Vendas", int(ag_data['Vendas']))
    c3.metric("Hit Ratio", f"{ag_data['HR %']:.1f}%")
    c4.metric("TMC", format_hms(ag_data['TMC_Sec']))

    # Gráficos
    g1, g2 = st.columns(2)
    with g1:
        st.markdown("**📅 Volume por Hora**")
        df_ag_h = df_tel[df_tel['Agente_NIF'] == nif_ag].copy()
        df_ag_h = df_ag_h[(df_ag_h['Data_Hora_DT'].dt.date >= start_date) & (df_ag_h['Data_Hora_DT'].dt.date <= end_date)]
        if not df_ag_h.empty:
            df_ag_h['Hora'] = df_ag_h['Data_Hora_DT'].dt.hour
            hourly = df_ag_h.groupby(['Hora', 'Campanha_Mapeada']).size().reset_index(name='Vol')
            
            # Preparar Tooltips (Hover)
            hourly['Hora_Intervalo'] = hourly['Hora'].astype(str) + "-" + (hourly['Hora'] + 1).astype(str) + "h"
            hourly = hourly.rename(columns={'Vol': 'Chamadas', 'Campanha_Mapeada': 'Campanha'})
            
            fig_h = px.bar(hourly, x='Hora', y='Chamadas', color='Campanha', template=plotly_template, 
                           hover_data={'Hora_Intervalo': True})
            fig_h.update_traces(hovertemplate="<b>Campanha:</b> %{fullData.name}<br><b>Hora:</b> %{customdata[0]}<br><b>Chamadas:</b> %{y}<extra></extra>")
            fig_h.update_layout(xaxis_title="Hora do Dia", yaxis_title="Volume de Chamadas")
            
            st.plotly_chart(fig_h, use_container_width=True)
            
    with g2:
        st.markdown("**📊 Comparativo Equipa (Média)**")
        tl = ag_data['Team Leader']
        avg_eq = kpi_table[kpi_table['Team Leader'] == tl].mean(numeric_only=True)
        
        from plotly.subplots import make_subplots
        fig = make_subplots(rows=1, cols=3, subplot_titles=("Atendidas", "Vendas", "Hit Ratio %"))
        
        c_ag = accent
        c_eq = '#4b5563'
        
        fig.add_trace(go.Bar(name="Agente", x=['Agente', 'Equipa'], y=[ag_data['Vol_Atendidas'], avg_eq['Vol_Atendidas']], marker_color=[c_ag, c_eq], text=[ag_data['Vol_Atendidas'], round(avg_eq['Vol_Atendidas'],1)]), row=1, col=1)
        fig.add_trace(go.Bar(name="Vendas", x=['Agente', 'Equipa'], y=[ag_data['Vendas'], avg_eq['Vendas']], marker_color=[c_ag, c_eq], text=[ag_data['Vendas'], round(avg_eq['Vendas'],1)]), row=1, col=2)
        fig.add_trace(go.Bar(name="HR %", x=['Agente', 'Equipa'], y=[ag_data['HR %'], avg_eq['HR %']], marker_color=[c_ag, c_eq], text=[f"{ag_data['HR %']:.2f}%", f"{avg_eq['HR %']:.2f}%"]), row=1, col=3)
        
        fig.update_traces(
            texttemplate="<b>%{text}</b>", 
            textposition='outside', 
            textfont=dict(size=14, color="white"),
            width=0.4
        )
        
        fig.update_layout(showlegend=False, height=350, template=plotly_template, margin=dict(l=20,r=20,t=40,b=20))
        st.plotly_chart(fig, use_container_width=True)

    # --- Inspecção de Vendas ---
    with st.expander("💰 Lista de Vendas Identificadas"):
        if not df_filtered_doc.empty:
            # Match 1: NIF direto
            v_id_col = COLUMNS_DOC['venda_id']
            vendas_nif = df_filtered_doc[df_filtered_doc['Agente_NIF'] == nif_ag].copy()
            
            # Match 2: Cruzamento Telefónico
            def clean_phone(p):
                if pd.isna(p): return ""
                return re.sub(r'\D', '', str(p))
            
            tels_raw = df_filtered_tel[df_filtered_tel['Agente_NIF'] == nif_ag][COLUMNS_TELEFONIA['phone']].unique()
            tels_validos = [clean_phone(t) for t in tels_raw if clean_phone(t) != ""]
            
            # Apply cleaning to the contact column in df_filtered_doc for matching
            df_filtered_doc_cleaned = df_filtered_doc.copy()
            df_filtered_doc_cleaned['clean_contacto'] = df_filtered_doc_cleaned[COLUMNS_DOC['contacto']].apply(clean_phone)
            
            vendas_tel = df_filtered_doc_cleaned[df_filtered_doc_cleaned['clean_contacto'].isin(tels_validos)].copy()
            
            vendas_final = pd.concat([vendas_nif, vendas_tel]).drop_duplicates(subset=[v_id_col])
            
            if not vendas_final.empty:
                st.write(f"Total de {len(vendas_final)} vendas únicas atribuídas:")
                st.dataframe(vendas_final[['Data_Hora_DT', COLUMNS_DOC['contacto'], COLUMNS_DOC['nic'], v_id_col]], use_container_width=True)
            else:
                st.info("Nenhuma venda encontrada para este agente neste período.")
        else:
            st.info("Ficheiro DOC não carregado ou vazio.")
