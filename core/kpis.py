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

def calculate_productivity(talk_time, login_time):
    if login_time <= 0:
        return 0
    return (talk_time / login_time) * 100
