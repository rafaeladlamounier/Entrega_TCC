import pandas as pd
import googlemaps
import numpy as np
import os
from datetime import datetime, timedelta

# Define a chave da API do Google Maps
API_KEY = 'tirei a key do google por motivos de segurança'

#CAMINHOS ENTRADA E SAÍDA
INPUT_CSV = 'ENTREGA/0. DADOS/amostra/estabelecimentos_bh_amostra_bairros.csv' 
DIST_FILE = 'ENTREGA/0. DADOS/matrizes_amostra/npy/matriz_distancias.npy' 
TIME_FILE = 'ENTREGA/0. DADOS/matrizes_amostra/npy/matriz_tempos.npy'  

# Define parâmetros da simulação, terça-feira às 10:00
HORA = 10  # 10:00
DIA = 1  # terça-feira

# Calcula a próxima ocorrência do dia especificado
hoje = datetime.now()
dias_ate = (DIA - hoje.weekday() + 7) % 7 
data_partida = hoje + timedelta(days=dias_ate)

# Define o horário exato da partida
HORA_PARTIDA = data_partida.replace(hour=HORA, minute=0, second=0, microsecond=0)

# Carrega o arquivo CSV com os locais
try:
    df = pd.read_csv(INPUT_CSV)
    print(f"Carregados {len(df)} locais")
except:
    print("Erro ao ler arquivo")
    exit()

# Cria lista de tuplas com as coordenadas (latitude, longitude)
coords = list(zip(df['latitude'], df['longitude']))
n = len(coords) 

# Afim de manter o progresso anterior e evitar uso de recursos de forma desnecessária:
# Verifica se já existe progresso anterior
if os.path.exists(TIME_FILE):
    print("Continuando progresso anterior")
    # Carrega matrizes já processadas
    mat_dist = np.load(DIST_FILE)
    mat_tempo = np.load(TIME_FILE)
else:
    print("Início")
    # Cria matrizes vazias (n x n)
    mat_dist = np.zeros((n, n))
    mat_tempo = np.zeros((n, n))

# Inicializa o cliente da API do Google Maps
gmaps = googlemaps.Client(key=API_KEY)

print("\nconectado")
try:
    for i in range(n):
        nome = df.loc[i, 'name']
        
        # Verifica se esta linha já foi processada (soma diferente de zero)
        if np.sum(mat_tempo[i]) != 0:
            print(f"Pulando {i+1}/{n} ({nome})")
            continue
        
        print(f"Processando {i+1}/{n}: {nome}")
        origem = coords[i]
        
        # Processa destinos em lotes de 25, devido a limitação das chamadas da API
        for j_ini in range(0, n, 25):
            j_fim = min(j_ini + 25, n)  #  último índice do lote
            destinos = coords[j_ini:j_fim]  # parcela dos destinos
            
            # Requisição à API Distance Matrix
            res = gmaps.distance_matrix(
                origins=[origem],
                destinations=destinos,
                mode="driving", 
                departure_time=HORA_PARTIDA 
            )
            
            # Processa os resultados
            if res['rows'][0]['elements']:
                for offset, el in enumerate(res['rows'][0]['elements']):
                    j = j_ini + offset  # Índice absoluto do destino
                    if el['status'] == 'OK':  # Se a rota foi encontrada
                        # distância em metros
                        mat_dist[i, j] = el['distance']['value']
                        # tempo em segundos (com tráfego se disponível)
                        mat_tempo[i, j] = el.get('duration_in_traffic', el['duration'])['value']
                    else:  # Se a rota não foi encontrada
                        mat_dist[i, j] = -1
                        mat_tempo[i, j] = -1
        
        np.save(DIST_FILE, mat_dist)
        np.save(TIME_FILE, mat_tempo)
    
    print("\nConcluído")
except Exception as e:
    print(f"Erro: {e}")
    print("Execute novamente para continuar de onde parou")