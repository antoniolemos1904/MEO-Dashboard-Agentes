import pandas as pd
import numpy as np
from core.config import *

def format_hms(seconds):
    if pd.isna(seconds) or seconds <= 0:
        return "00:00:00"
    h = int(seconds // 3600)
    m = int((seconds % 3600) // 60)
    s = int(seconds % 60)
    return f"{h:02d}:{m:02d}:{s:02d}"

def calculate_login_time(df_logs, campanha):
    """
    Calcula o tempo de login considerando Join/Leave estritamente para a campanha selecionada.
    """
    if df_logs.empty:
        return {}
        
    # Filtrar apenas eventos da campanha selecionada
    df = df_logs[df_logs['Campanha_Mapeada'] == campanha]
    
    df = df.sort_values(['Agente_NIF', 'Data_Hora_DT'])
    
    agent_times = {}
    
    for (agente, dia), group in df.groupby(['Agente_NIF', df['Data_Hora_DT'].dt.date]):
        joins = group[group[COLUMNS_LOGS['evento']].str.lower().isin(['join', 'login'])]
        leaves = group[group[COLUMNS_LOGS['evento']].str.lower().isin(['leave', 'logout'])]
        
        day_seconds = 0
        for _, join_row in joins.iterrows():
            join_time = join_row['Data_Hora_DT']
            next_leave = leaves[leaves['Data_Hora_DT'] > join_time]
            
            if not next_leave.empty:
                leave_time = next_leave.iloc[0]['Data_Hora_DT']
            else:
                # Fallback: Fim do dia ou Join seguinte
                leave_time = join_time.replace(hour=21, minute=0, second=0)
                if leave_time < join_time:
                    leave_time = join_time
            
            day_seconds += (leave_time - join_time).total_seconds()
            
        agent_times[agente] = agent_times.get(agente, 0) + day_seconds
            
    return agent_times

def calculate_lunch_time(df_logs):
    """
    Calcula o tempo total de almoço por agente.
    Emparelha eventos de 'break' com 'Almoço': 1º=início, 2º=fim, 3º=início, 4º=fim, etc.
    """
    if df_logs.empty:
        return {}
    
    # Filtrar apenas eventos de break + Almoço
    evt_col = COLUMNS_LOGS['evento']
    sub_col = COLUMNS_LOGS['campanha']  # Event Subtype Name
    
    mask_break = df_logs[evt_col].astype(str).str.lower() == 'break'
    mask_lunch = df_logs[sub_col].astype(str).str.lower().str.contains('almo', na=False)
    df_lunch = df_logs[mask_break & mask_lunch].copy()
    
    if df_lunch.empty:
        return {}
    
    df_lunch = df_lunch.sort_values(['Agente_NIF', 'Data_Hora_DT'])
    
    agent_lunch = {}
    
    for (agente, dia), group in df_lunch.groupby(['Agente_NIF', df_lunch['Data_Hora_DT'].dt.date]):
        events = group['Data_Hora_DT'].tolist()
        day_lunch = 0
        # Emparelhar: índice 0=início, 1=fim, 2=início, 3=fim...
        for i in range(0, len(events) - 1, 2):
            start = events[i]
            end = events[i + 1]
            day_lunch += (end - start).total_seconds()
        
        agent_lunch[agente] = agent_lunch.get(agente, 0) + day_lunch
    
    return agent_lunch

def calculate_productivity(talk_time, login_time):
    if login_time <= 0:
        return 0
    return (talk_time / login_time) * 100
