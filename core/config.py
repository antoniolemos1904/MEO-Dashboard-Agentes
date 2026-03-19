import re

# Mapeamento de Campanhas baseado no Database Name
def map_campaign(db_name):
    if not isinstance(db_name, str):
        return "Desconhecida"
    
    db_name = db_name.upper()
    
    # Lógica baseada em nomes amigáveis (comum nos Logs)
    if "ALTO VALOR" in db_name: return "Alto Valor"
    if "REDE MÓVEL" in db_name or "REDE MOVEL" in db_name or "RCES" in db_name: return "Rede Móvel"
    if "FIBRA" in db_name: return "Fibra"
    if "MÓVEL INATIVOS" in db_name or "MOVEL INATIVOS" in db_name: return "Móvel Inativos"

    # Prefixos Diretos para Móvel Inativos (Telefonia)
    if any(db_name.startswith(prefix) for prefix in ["CA809", "CA805", "CA769", "CA767", "CA801"]):
        return "Móvel Inativos"
    
    # Lógica OB8 com números específicos (Telefonia)
    if db_name.startswith("OB8") or db_name.startswith('"OB8'):
        db_clean = db_name.replace('"', '')
        if "5571" in db_clean:
            return "Móvel Inativos"
        elif "5564" in db_clean:
            return "Rede Móvel"
        elif "5580" in db_clean:
            return "Alto Valor"
        elif any(num in db_clean for num in ["5625", "5318"]):
            return "Fibra"
            
    return "Outra/Manual"

# Configurações de Colunas
COLUMNS_TELEFONIA = {
    "data_hora": "Call start",
    "agente": "Agent User Name",
    "talk_time": "Talk Time",
    "campanha_tecnica": "Campaign Name",
    "phone": "Phone",
    "nic": "nic [field65]",
    "nic_efectivo": "nic_efectivo [field51]",
    "outcome": "Call Outcome name",
    "db_name": "Database Name"
}

COLUMNS_LOGS = {
    "agente": "Username",
    "evento": "Event Type",
    "data_hora": "Event Date",
    "campanha": "Event Subtype Name"
}

COLUMNS_DOC = {
    "agente": "NIF_VENDEDOR",
    "evento": "EVENTO",
    "venda_id": "VENDAID",
    "nic": "NIC",
    "familia": "FAMILIA",
    "data_ref": "DATA_REF",
    "contacto": "CONTACTO_CLIENTE"
}

# Filtros
DOC_FAMILIAS_VALIDAS = ["BLFIXA", "MEOFIBRA", "MEOSAT", "VOZFIXA"]
CADASTRO_CAMPANHA_FILTRO = "TV"
TELEFONIA_EXCLUDE_PREFIX = "NEW LEAD OUTBOUND"
