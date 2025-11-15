import pandas as pd
import numpy as np
import math


caminho_matriz_distancias = 'ENTREGA/0. DADOS/matrizes_amostra/npy/matriz_distancias.npy'
caminho_matriz_tempos = 'ENTREGA/0. DADOS/matrizes_amostra/npy/matriz_tempos.npy'
caminho_amostra = 'ENTREGA/0. DADOS/amostra/estabelecimentos_bh_amostra_bairros.csv'

saida_savings_dist = 'ENTREGA/0. DADOS/matrizes_amostra/npy/savings_distancias.npy'
saida_savings_tempo = 'ENTREGA/0. DADOS/matrizes_amostra/npy/savings_tempos.npy'
saida_savings_csv = 'ENTREGA/0. DADOS/matrizes_amostra/csv/savings_list_ranked.csv'

saida_dedicadas = 'ENTREGA/0. DADOS/rotas/hibrido/preclusterizacao/rotas_dedicadas_excesso.csv'
saida_clusterizar = 'ENTREGA/0. DADOS/rotas/hibrido/preclusterizacao/pdvs_para_clusterizar.csv'

# RESTRIÇÃO:
CAPACIDADE_MAXIMA = 12000.0  

# Peso em Kg e volume em m³ para cada produto, por caixa
SPECS = {
    'GARRAFA': {'peso': 23.0, 'vol': 0.050},  
    'PET':     {'peso': 12.3, 'vol': 0.035},  
    'LATA':    {'peso': 4.5,  'vol': 0.008}  
}
# Define a ordem de prioridade para carregar o caminhão (mais pesado primeiro)
PRIORIDADE = ['GARRAFA', 'PET', 'LATA']

# FASE 1: CÁLCULO DE SAVINGS (Algoritmo de Clarke-Wright)
print("FASE 1: CÁLCULO DE SAVINGS")

# Carrega as matrizes de distância e tempo
mat_dist = np.load(caminho_matriz_distancias)
mat_tempo = np.load(caminho_matriz_tempos)
df = pd.read_csv(caminho_amostra, sep=';')

n = len(df) 
# Matrizes nxn para armazenar os savings
sav_dist = np.zeros((n, n))
sav_tempo = np.zeros((n, n))
lista_sav = []  # para armazenagem via lista

# Calcula savings para todos os pares de PDVs (i, j)
for i in range(1, n):  # pula o CDD (índice 0)
    for j in range(1, n):
        if i == j:  # ignora na diagonal (origem e destino iguais)
            continue
        
        # Distâncias e tempos individuais
        d_i_cdd = mat_dist[i, 0]  # Distância do PDV i ao CDD
        t_i_cdd = mat_tempo[i, 0]  # Tempo do PDV i ao CDD
        d_cdd_j = mat_dist[0, j]  # Distância do CDD ao PDV j
        t_cdd_j = mat_tempo[0, j]  # Tempo do CDD ao PDV j
        d_i_j = mat_dist[i, j]    # Distância direta entre PDV i e j
        t_i_j = mat_tempo[i, j]    # Tempo direto entre PDV i e j
        
        # Fórmula adaptada de savings para matriz assimétrica: (i->CDD + CDD->j) - (i->j)
        sav_d = d_i_cdd + d_cdd_j - d_i_j
        sav_t = t_i_cdd + t_cdd_j - t_i_j
        
        sav_dist[i, j] = sav_d
        sav_tempo[i, j] = sav_t
        
        # Adiciona à lista com códigos dos PDVs, conventendo para km e min
        lista_sav.append({
            'COD_PDV_Origem': df.loc[i, 'COD PDV'],
            'COD_PDV_Destino': df.loc[j, 'COD PDV'],
            'saving_distancia_km': sav_d / 1000,
            'saving_tempo_min': sav_t / 60  
        })

# Salva as matrizes de savings
np.save(saida_savings_dist, sav_dist)
np.save(saida_savings_tempo, sav_tempo)

# Cria DataFrame, ordena por saving de distância (decrescente) e salva
df_sav = pd.DataFrame(lista_sav)
df_sav = df_sav.sort_values(by='saving_distancia_km', ascending=False).reset_index(drop=True)
df_sav.to_csv(saida_savings_csv, index=False, sep=';')

print(f"Savings: {len(df_sav)} pares")

# FASE 0: PARTICIONAMENTO DE CARGAS (separar PDVs que excedem capacidade)
print("FASE 0: PARTICIONAMENTO DE CARGAS")

# Calcula peso e volume total de uma demanda
def calcular_carga(demandas):
    peso = sum(demandas.get(f'demanda_{p}', 0) * SPECS[p]['peso'] for p in PRIORIDADE)
    vol = sum(demandas.get(f'demanda_{p}', 0) * SPECS[p]['vol'] for p in PRIORIDADE)
    return peso, vol

# Calcula o tempo de atendimento em um PDV
def tempo_atendimento(dados):
    if dados.get('type') == 'CDD': 
        return 0
    
    tempo_base = 14.0  #Fixo
    tempo_fila = 60.0 if dados.get('type') in ['supermarket', 'alcohol'] else 0  #Espera
    cx = sum(dados.get(f'demanda_{p}', 0) for p in PRIORIDADE)  
    tempo_desc = (cx * 20.0) / 60.0  #Descarga
    tempo_ret = (dados.get('demanda_GARRAFA', 0) * 20.0) / 60.0  #Retornárveis
    
    return round(tempo_base + tempo_fila + tempo_desc + tempo_ret, 2)

# Função que enche um caminhão respeitando o limite de peso e de tempo (volume não é necessário)
def encher_caminhao(demandas):
    peso_disp = CAPACIDADE_MAXIMA  # Peso disponível no caminhão (RESTRIÇÃO)
    carga = {f'demanda_{p}': 0 for p in PRIORIDADE}  # Carga que vai no caminhão
    resto = demandas.copy()  # Demanda restante
    
    # Processa produtos em ordem de prioridade (mais pesado primeiro)
    for prod in PRIORIDADE:
        spec = SPECS[prod]
        key = f'demanda_{prod}'
        
        # Calcula quantas caixas cabem por peso (ÚNICA RESTRIÇÃO)
        por_peso = math.floor(peso_disp / (spec['peso'] + 1e-9))  # +1e-9 evita divisão por zero
        # Quantidade que efetivamente vai: mínimo entre demanda e limite de peso
        qtd = max(0, min(resto.get(key, 0), por_peso))
        #adiciona a carga ao caminhão, remove do restante da demanda e atualiza
        carga[key] = qtd
        resto[key] -= qtd
        peso_disp -= qtd * spec['peso']
    return carga, resto

# Carrega os dados dos PDVs
df_pdvs = pd.read_csv(caminho_amostra, sep=';')

# Listas para armazenar PDVs categorizados
dedicadas = []  # Rotas dedicadas (PDVs que precisam de múltiplos caminhões)
clusterizar = []  # PDVs para clusterizar 

#Para cada PDV, decide se vai para clusterização ou precisa de rotas dedicadas
for idx, row in df_pdvs.iterrows():
    if row['COD PDV'] == '00':
        clusterizar.append(row.to_dict())
        continue
    peso_total = row['peso_total_kg']
    fator = peso_total / CAPACIDADE_MAXIMA
    if fator <= 1.0:
        clusterizar.append(row.to_dict())
    else:
        # Precisa de múltiplos caminhões (rotas dedicadas)
        demandas = {f'demanda_{p}': row[f'demanda_{p}'] for p in PRIORIDADE}
        num_cam = 0  # Contador de caminhões necessários
        while fator > 1.0 :
            num_cam += 1
            carga, demandas = encher_caminhao(demandas)
            peso_cam, vol_cam = calcular_carga(carga) 
            # Registra a rota dedicada
            dedicadas.append({
                'COD PDV': row['COD PDV'],
                'name': row['name'],
                'rota_dedicada_num': num_cam,  
                'carga_lata': carga['demanda_LATA'],
                'carga_pet': carga['demanda_PET'],
                'carga_garrafa': carga['demanda_GARRAFA'],
                'peso_total_caminhao': round(peso_cam, 2),
                'volume_total_caminhao': round(vol_cam, 2) 
            })
            # Recalcula o fator para a demanda restante
            peso_rest, vol_rest = calcular_carga(demandas) 
            fator = peso_rest / CAPACIDADE_MAXIMA
        
        # Se ainda sobrou demanda mas cabe em 1 caminhão, vai para clusterização
        if any(d > 0 for d in demandas.values()):
            resto_pdv = row.to_dict()
            # Atualiza as demandas com o que sobrou
            for p in PRIORIDADE:
                resto_pdv[f'demanda_{p}'] = demandas[f'demanda_{p}']
            
            # Recalcula peso, volume e tempo para a demanda restante
            peso_r, vol_r = calcular_carga(demandas) 
            tempo_r = tempo_atendimento(resto_pdv)
            
            resto_pdv['peso_total_kg'] = round(peso_r, 3)
            resto_pdv['volume_total_m3'] = round(vol_r, 3) 
            resto_pdv['tempo_servico_min'] = round(tempo_r, 2)
            
            clusterizar.append(resto_pdv)

# Salva o arquivo de rotas dedicadas 
df_ded = pd.DataFrame(dedicadas)
if not df_ded.empty:
    df_ded.to_csv(saida_dedicadas, index=False, sep=';')
    print(f"Dedicadas: {len(df_ded)} rotas")

# Salva o arquivo de PDVs para clusterizar
df_clust = pd.DataFrame(clusterizar)
df_clust.to_csv(saida_clusterizar, index=False, sep=';')
print(f"Para clusterizar: {len(df_clust)} PDVs")

print("Concluído")