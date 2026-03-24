import pandas as pd
import glob
import os
import re
import io
import streamlit as st
from datetime import datetime
from core.config import *

FOLDER_IDS = {
    "telefonia": "1Nvg1o18vetcTwA4IbgUkQhZeeF0WlqvC",
    "logs": "1Uv7PPM8v3SyJ9UU7SOLeIaJJFCovI7G0",
    "doc": "14qb3q8cQTmVVBdw626YBNsrgghEeWg19"
}

def get_drive_service():
    try:
        from google.oauth2 import service_account
        from googleapiclient.discovery import build
        if "google_drive" in st.secrets:
            info = dict(st.secrets["google_drive"])
            credentials = service_account.Credentials.from_service_account_info(info)
            return build('drive', 'v3', credentials=credentials)
        else:
            st.error("❌ 'google_drive' não encontrado nos st.secrets! Verifica as definições avançadas na Cloud.")
            return None
    except Exception as e:
        st.error(f"❌ Erro de Autenticação Google API: {e}")
        return None
    return None

def fetch_files_from_source(path_dir, folder_key, exts=["*.csv"]):
    dfs = []
    
    # Detetar se existem ficheiros locais antes de assumir Modo Local
    local_files = []
    if os.path.exists(path_dir):
        for ext in exts:
            local_files.extend(glob.glob(os.path.join(path_dir, "**", ext), recursive=True))
            
    if len(local_files) > 0:
        # --- MODO LOCAL ---
        for f in local_files:
            try:
                if f.lower().endswith(('.xlsx', '.xls')): df = pd.read_excel(f)
                else: df = pd.read_csv(f, sep=';', encoding='utf-8-sig')
                df.columns = df.columns.str.strip().str.replace('"', '')
                dfs.append(df)
            except Exception: continue
    else:
        # --- MODO CLOUD ---
        service = get_drive_service()
        if not service: return []  # Aborta imediatamente se não se conseguiu ligar
        
        folder_id = FOLDER_IDS.get(folder_key)
        if folder_id:
            query = f"'{folder_id}' in parents and trashed = false"
            try:
                results = service.files().list(q=query, pageSize=1000, fields="files(id, name)").execute()
                items = results.get('files', [])
                if not items:
                    st.warning(f"⚠️ A pasta '{folder_key}' ({folder_id}) foi acedida, mas não tem ficheiros CSV!")
                
                valid_exts = [e.replace('*', '') for e in exts]
                for item in items:
                    name = item['name'].lower()
                    if not any(name.endswith(e.lower()) for e in valid_exts):
                        continue
                    
                    try:
                        request = service.files().get_media(fileId=item['id'])
                        file_content = io.BytesIO(request.execute())
                        if name.endswith(('.xlsx', '.xls')): df = pd.read_excel(file_content)
                        else: df = pd.read_csv(file_content, sep=';', encoding='utf-8-sig')
                        
                        df.columns = df.columns.str.strip().str.replace('"', '')
                        dfs.append(df)
                    except Exception as e:
                        st.error(f"Erro ao ler o ficheiro {name}: {e}")
                        continue
            except Exception as e:
                st.error(f"❌ Erro ao aceder à pasta {folder_key} na Google Drive API: {e}")
                
    return dfs

def clean_nif(nif):
    if pd.isna(nif) or nif == "": return ""
    s = re.sub(r'\D', '', str(nif))
    return s[-9:] if len(s) >= 9 else s

def load_telefonia(path_dir):
    dfs = fetch_files_from_source(path_dir, "telefonia", ["*.csv"])
    if not dfs:
        return pd.DataFrame(columns=['Agente_NIF', 'Campanha_Mapeada', 'Data_Hora_DT', COLUMNS_TELEFONIA['db_name'], COLUMNS_TELEFONIA['talk_time'], COLUMNS_TELEFONIA['phone'], COLUMNS_TELEFONIA['nic'], COLUMNS_TELEFONIA['nic_efectivo']])
        
    df = pd.concat(dfs, ignore_index=True)
    df['Agente_NIF'] = df[COLUMNS_TELEFONIA['agente']].apply(clean_nif)
    df['Campanha_Mapeada'] = df[COLUMNS_TELEFONIA['db_name']].apply(map_campaign)
    df = df[df[COLUMNS_TELEFONIA['talk_time']] > 0]
    df['Data_Hora_DT'] = pd.to_datetime(df[COLUMNS_TELEFONIA['data_hora']], format='mixed', errors='coerce')
    return df

def load_logs(path_dir):
    dfs = fetch_files_from_source(path_dir, "logs", ["*.csv"])
    if not dfs: return pd.DataFrame()
    df = pd.concat(dfs, ignore_index=True)
    
    def clean_log_u(u):
        if pd.isna(u): return ""
        u_str = str(u).upper()
        if u_str.startswith('U'): u_str = u_str[1:]
        return clean_nif(u_str)

    df['Agente_NIF'] = df[COLUMNS_LOGS['agente']].apply(clean_log_u)
    df['Campanha_Mapeada'] = df[COLUMNS_LOGS['campanha']].apply(map_campaign)
    df['Data_Hora_DT'] = pd.to_datetime(df[COLUMNS_LOGS['data_hora']], format='mixed', errors='coerce')
    return df

def load_doc(path_dir):
    dfs_raw = fetch_files_from_source(path_dir, "doc", ["*.csv", "*.xlsx", "*.xls"])
    dfs = []
    v_id_col = COLUMNS_DOC['venda_id']
    
    for df in dfs_raw:
        if v_id_col in df.columns:
            df = df[df[v_id_col].notna() & (df[v_id_col].astype(str).str.strip() != "")]
            dfs.append(df)
            
    if not dfs: return pd.DataFrame()
    df = pd.concat(dfs, ignore_index=True)
    
    # Conversões e Filtros
    if COLUMNS_DOC['agente'] in df.columns:
        df['Agente_NIF'] = df[COLUMNS_DOC['agente']].apply(clean_nif)
    if COLUMNS_DOC['data_ref'] in df.columns:
        df['Data_Hora_DT'] = pd.to_datetime(df[COLUMNS_DOC['data_ref']], errors='coerce')
    if COLUMNS_DOC['evento'] in df.columns:
        df = df[df[COLUMNS_DOC['evento']].astype(str).str.upper() == 'VB']
    if COLUMNS_DOC['familia'] in df.columns:
        validas = [f.upper() for f in DOC_FAMILIAS_VALIDAS]
        df = df[df[COLUMNS_DOC['familia']].astype(str).str.upper().isin(validas)]
        
    return df.drop_duplicates(subset=[v_id_col])

def get_sales_details(df_tel, df_doc):
    """
    Atribui vendas (DOC) à campanha representada por df_tel.
    Passo 1: Filtrar DOC pelos contactos (Phone/NIC) que foram chamados nesta campanha.
    Passo 2: Usar o NIF_VENDEDOR do DOC como atribuição ao agente.
    """
    if df_tel.empty or df_doc.empty: return pd.DataFrame()
    
    v_id = COLUMNS_DOC['venda_id']
    
    def prep_phone(val):
        """Normaliza telefone: remove não-dígitos e mantém últimos 9 dígitos (formato PT)."""
        if pd.isna(val) or val == "": return None
        s = re.sub(r'\D', '', str(val))
        if s == "" or s == "0": return None
        return s[-9:] if len(s) >= 9 else s
    
    def prep_nic(val):
        """Normaliza NIC: remove não-dígitos."""
        if pd.isna(val) or val == "": return None
        s = re.sub(r'\D', '', str(val))
        return s if s != "" and s != "0" else None

    # --- Preparar Telefonia (já filtrada por campanha e período) ---
    df_t = df_tel.copy()
    if COLUMNS_TELEFONIA['phone'] in df_t.columns:
        df_t['_phone_norm'] = df_t[COLUMNS_TELEFONIA['phone']].apply(prep_phone)
    if COLUMNS_TELEFONIA['nic'] in df_t.columns:
        df_t['_nic_norm'] = df_t[COLUMNS_TELEFONIA['nic']].apply(prep_nic)

    # --- Preparar DOC ---
    df_d = df_doc.copy()
    if COLUMNS_DOC['contacto'] in df_d.columns:
        df_d['_phone_norm'] = df_d[COLUMNS_DOC['contacto']].apply(prep_phone)
    if COLUMNS_DOC['nic'] in df_d.columns:
        df_d['_nic_norm'] = df_d[COLUMNS_DOC['nic']].apply(prep_nic)
    if v_id in df_d.columns:
        df_d[v_id] = df_d[v_id].astype(str).str.strip()

    # === PASSO 1: Identificar contactos desta campanha ===
    campaign_phones = set(df_t['_phone_norm'].dropna().unique()) - {None, ""}
    campaign_nics = set(df_t['_nic_norm'].dropna().unique()) - {None, ""}
    
    # Filtrar DOC: manter apenas vendas cujo contacto OU NIC foi chamado nesta campanha
    phone_hit = df_d['_phone_norm'].isin(campaign_phones) if campaign_phones else pd.Series(False, index=df_d.index)
    nic_hit = df_d['_nic_norm'].isin(campaign_nics) if campaign_nics else pd.Series(False, index=df_d.index)
    
    matches = df_d[phone_hit | nic_hit].drop_duplicates(subset=[v_id]).copy()
    
    # === PASSO 2: Atribuição ao Agente ===
    # O NIF_VENDEDOR do DOC já indica quem fez a venda (Agente_NIF já está no df_d)
    # Não precisamos de alterar a atribuição. O agente é quem registou a venda.
    
    return matches

def get_sales_per_agent(df_tel, df_doc):
    matches = get_sales_details(df_tel, df_doc)
    if matches.empty: return pd.Series(0, dtype=int)
    return matches.groupby('Agente_NIF')[COLUMNS_DOC['venda_id']].nunique()

def get_cadastro(sheet_url):
    local_path = os.path.join(os.path.dirname(__file__), "..", "data", "cadastro", "cadastro.csv")
    df = pd.read_csv(local_path if os.path.exists(local_path) else sheet_url, encoding='utf-8-sig')
    df.columns = df.columns.str.strip()
    c_nif = 'NIF' if 'NIF' in df.columns else 'Nif'
    df['Agente_NIF'] = df[c_nif].apply(clean_nif)
    return df
