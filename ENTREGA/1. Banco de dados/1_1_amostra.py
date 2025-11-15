import requests
import pandas as pd
import time
from geopy.geocoders import Nominatim
import random
import os

# CAMINHOS SAÍDA
saida_amostra = 'ENTREGA/0. DADOS/amostra/estabelecimentos_bh_amostra_bairros.csv'
saida_completo = 'ENTREGA/0. DADOS/amostra/estabelecimentos_bh_completo.csv'

# Define as coordenadas e informações do Centro de Distribuição (CDD)
CDD = {
    'id': 0, 'name': 'CDD', 'type': 'CDD',
    'latitude': -19.911125, 'longitude': -44.049394, 'bairro': 'Cincao'
}

#------------- AMOSTRA -----------------------------------------------------------------
# Função que busca PDVs no OpenStreetMap usando a API Overpass
def buscar_pdvs_osm(cidade, tipos):
    partes = []
    # Para cada tipo de estabelecimento, adiciona queries para nós e caminhos
    for t in tipos:
        partes.append(f'node[{t}](area.searchArea);')  # Busca nós (pontos)
        partes.append(f'way[{t}](area.searchArea);')   # Busca caminhos (áreas)
    
    # Monta a query completa do Overpass
    query = f"""
    [out:json][timeout:90];
    area[name="{cidade}"]->.searchArea;
    ({''.join(partes)});
    out center;
    """
    
    # Faz a requisição para a API Overpass
    resp = requests.get("http://overpass-api.de/api/interpreter", params={'data': query})
    return resp.json() if resp.status_code == 200 else None  # Retorna JSON se sucesso


# Função que busca o nome do bairro usando geocodificação reversa, puramente por curiosidade, para analise visual dos bairros sortidos.
def buscar_bairro(lat, lon, geo):
    try:
        time.sleep(1)  # Aguarda 1 segundo para respeitar limite da API
        # Faz geocodificação reversa (coordenadas -> endereço)
        loc = geo.reverse((lat, lon), exactly_one=True, language='pt-BR')
        if loc:
            addr = loc.raw.get('address', {})
            # Tenta obter 'suburb' ou 'neighbourhood' do endereço
            return addr.get('suburb', addr.get('neighbourhood', 'Desconhecido'))
        return 'Desconhecido'
    except:
        return 'Desconhecido'

# Função principal que coleta dados do OpenStreetMap
def coletar_osm():
    print("Buscando PDVs")
    # Define os tipos de estabelecimentos a buscar
    tipos = ["amenity=bar", "amenity=restaurant", "shop=supermarket", "shop=alcohol"]
    dados = buscar_pdvs_osm("Belo Horizonte", tipos)
    
    if not dados:
        return None
    
    pdvs = []
    # Para cada elemento retornado pela API
    for el in dados.get('elements', []):
        # Obtém latitude e longitude (pode estar em 'lat'/'lon' ou em 'center')
        lat = el.get('lat') or el.get('center', {}).get('lat')
        lon = el.get('lon') or el.get('center', {}).get('lon')
        if not lat or not lon: 
            continue
        
        # Obtém as tags (metadados) do elemento
        tags = el.get('tags', {})
        pdvs.append({
            'id': el['id'],
            'name': tags.get('name', 'Sem nome'),
            'type': tags.get('amenity') or tags.get('shop'),  # Tipo do estabelecimento
            'latitude': lat,
            'longitude': lon
        })
    
    print(f"Encontrados {len(pdvs)} PDVs")
    # Salva todos os PDVs encontrados
    df = pd.DataFrame(pdvs)
    df.to_csv(saida_completo, index=False)
    
    # Filtra apenas PDVs com nome (válidos) e seleciona amostra aleatória de 50
    df_validos = df[df['name'] != 'Sem nome'].copy()
    amostra = df_validos.sample(n=min(50, len(df_validos)), random_state=42).reset_index(drop=True)
    
    # Busca o bairro de cada PDV da amostra
    print("Buscando bairros")
    geo = Nominatim(user_agent="route-opt")
    amostra['bairro'] = amostra.apply(lambda r: buscar_bairro(r['latitude'], r['longitude'], geo), axis=1)
    
    # Adiciona o CDD no início do DataFrame
    df_final = pd.concat([pd.DataFrame([CDD]), amostra], ignore_index=True)
    # Cria códigos sequenciais para os PDVs 
    df_final['COD PDV'] = [f'{i:02d}' for i in range(len(df_final))]
    df_final.to_csv(saida_amostra, index=False, sep=';')
    
    print(f"Amostra: {len(df_final)} PDVs")
    return df_final

#-----------------DEMANDA---------------------------------------------------------------:
# Função que gera demanda aleatória de caixas baseada no tipo de estabelecimento
def gerar_demanda(tipo):
    # Supermercados e lojas de bebida: alta demanda de todos os tipos
    if tipo in ['supermarket', 'alcohol']:
        return random.randint(51, 500), random.randint(51, 500), random.randint(51, 500)
    # Bares: média demanda de lata e garrafa, sem PET
    elif tipo == 'bar':
        return random.randint(31, 50), 0, random.randint(31, 50)
    # Restaurantes: baixa demanda de lata e garrafa, sem PET
    elif tipo == 'restaurant':
        return random.randint(0, 31), 0, random.randint(0, 31)
    return 0, 0, 0

# Função que adiciona colunas de demanda ao DataFrame
def adicionar_demandas(df):
    print("Gerando demandas")
    # Separa CDD dos PDVs
    cdd = df[df['name'] == 'CDD'].copy()
    pdvs = df[df['name'] != 'CDD'].copy()
    
    # Gera demandas aleatórias para cada PDV baseado no tipo
    demandas = pdvs['type'].apply(gerar_demanda)
    pdvs[['demanda_LATA', 'demanda_PET', 'demanda_GARRAFA']] = pd.DataFrame(demandas.tolist(), index=pdvs.index)
    
    # CDD tem demanda zero
    for col in ['demanda_LATA', 'demanda_PET', 'demanda_GARRAFA']:
        cdd[col] = 0
    
    # Recombina CDD com PDVs
    resultado = pd.concat([cdd, pdvs], ignore_index=True)
    # Recria os códigos dos PDVs
    resultado['COD PDV'] = [f'{i:02d}' for i in range(len(resultado))]
    
    # Reordena colunas para COD PDV aparecer primeiro
    cols = ['COD PDV'] + [c for c in resultado.columns if c != 'COD PDV']
    resultado = resultado[cols]
    resultado.to_csv(saida_amostra, index=False, sep=';')
    
    print("Demandas adicionadas")
    return resultado

#-------------------------------------TEMPO DE ATENDIMENTO------------
# Função que calcula o tempo de atendimento em um PDV
def tempo_atendimento(row):
    if row['type'] == 'CDD': 
        return 0
    
    # Componentes do tempo de atendimento
    tempo_base = 14.0  # Tempo fixo para chegada e preparação
    tempo_fila = 40.0 if row['type'] in ['supermarket', 'alcohol'] else 0  # Tempo de espera
    cx_total = row.get('demanda_LATA', 0) + row.get('demanda_PET', 0) + row.get('demanda_GARRAFA', 0)
    tempo_desc = (cx_total * 20.0) / 60.0  # Tempo para descarregar (20 segundos por caixa)
    tempo_ret = (row.get('demanda_GARRAFA', 0) * 20.0) / 60.0  # Tempo para recolher vasilhames (20 s por caixa)
    
    return round(tempo_base + tempo_fila + tempo_desc + tempo_ret, 2)

# Função que adiciona coluna de tempo de serviço ao DataFrame
def adicionar_tempos(df):
    print("Calculando tempos")
    df['tempo_servico_min'] = df.apply(tempo_atendimento, axis=1)
    df.to_csv(saida_amostra, index=False, sep=';')
    print("Tempos calculados")
    return df

#---------------PESO E VOLUME--------------------------
# Função que calcula peso e volume da carga de um PDV
def calcular_carga(row):
    # Obtém quantidade de cada tipo de embalagem
    lata = row.get('demanda_LATA', 0)
    pet = row.get('demanda_PET', 0)
    garrafa = row.get('demanda_GARRAFA', 0)
    
    # Calcula peso total (kg por caixa de cada tipo)
    peso = lata * 4.5 + pet * 12.3 + garrafa * 23.0
    # Calcula volume total (m³ por caixa de cada tipo)
    vol = lata * 0.008 + pet * 0.035 + garrafa * 0.05
    
    return round(peso, 3), round(vol, 3)

# Função que adiciona colunas de peso e volume ao DataFrame
def adicionar_peso_volume(df):
    print("Calculando peso e volume")
    # Aplica a função de cálculo para cada linha
    resultado = df.apply(calcular_carga, axis=1, result_type='expand')
    df[['peso_total_kg', 'volume_total_m3']] = resultado
    df.to_csv(saida_amostra, index=False, sep=';')
    print("Concluído")
    return df


# Função principal que executa todo o pipeline
def executar():
    print("Executando")
    
    try:
        # Passo 1: Coleta dados do OpenStreetMap
        df = coletar_osm()
        if df is None:
            print("Erro na coleta")
            return
        
        # Passo 2: Adiciona demandas aleatórias
        df = adicionar_demandas(df)
        # Passo 3: Calcula tempos de atendimento
        df = adicionar_tempos(df)
        # Passo 4: Calcula peso e volume
        df = adicionar_peso_volume(df)
        
        # Exibe resumo final
        print(f"\nArquivo: {saida_amostra}")
        print(f"Total: {len(df)} registros")
        
    except Exception as e:
        print(f"Erro: {e}")

# Ponto de entrada do script
if __name__ == "__main__":
    executar()