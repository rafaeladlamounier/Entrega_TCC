import pandas as pd
import numpy as np
import os

# RESTRIÇÕES
CAPACIDADE_MAXIMA = 12000.0
JORNADA_MAXIMA = 510.0

# CAMINHOS ENTRADA
caminho_pdvs = 'ENTREGA/0. DADOS/rotas/hibrido/preclusterizacao/pdvs_para_clusterizar.csv'
caminho_dedicadas = 'ENTREGA/0. DADOS/rotas/hibrido/preclusterizacao/rotas_dedicadas_excesso.csv'
caminho_matriz_tempos = 'ENTREGA/0. DADOS/matrizes_amostra/npy/matriz_tempos.npy'
caminho_matriz_distancias = 'ENTREGA/0. DADOS/matrizes_amostra/npy/matriz_distancias.npy'
caminho_amostra = 'ENTREGA/0. DADOS/amostra/estabelecimentos_bh_amostra_bairros.csv'

# CAMINHOS SAÍDA
saida_visualizacao = 'ENTREGA/0. DADOS/rotas/sem_clusterizacao/rotas_individuais_visualizacao.csv'
saida_relatorio = 'ENTREGA/0. DADOS/rotas/sem_clusterizacao/relatorio_rotas_individuais.csv'

# Tempo de atendimento de acordo com o tipo de estabelecimento e a demanda
def tempo_atendimento(dados_pdv):
    tipo = dados_pdv.get('type', 'CDD')
    if tipo == 'CDD': 
        return 0
    
    lata = dados_pdv.get('demanda_LATA', 0)
    pet = dados_pdv.get('demanda_PET', 0)
    garrafa = dados_pdv.get('demanda_GARRAFA', 0)
    
    tempo_base = 14.0
    tempo_fila = 60.0 if tipo in ['supermarket', 'alcohol'] else 0
    tempo_desc = (lata + pet + garrafa) * (20.0 / 60.0)
    tempo_ret = garrafa * (20.0 / 60.0)
    
    return round(tempo_base + tempo_fila + tempo_desc + tempo_ret, 2)

#Métricas de tempo e distância para a sequência de visitas
def calcular_metricas(seq, mat_tempo, mat_dist):
    if not seq:
        return {'sequencia': (), 'tempo_desloc_total': 0, 'dist_total': 0, 
                'dist_deslocamento': 0, 'dist_laco': 0}
    
    #Para rotas individuais, pega apenas o primeiro (e único) PDV
    idx = seq[0]
    #Calculo do tempo: CDD->PDV->CDD
    t = mat_tempo[0, idx] + mat_tempo[idx, 0]
    #Cálculo da distância CDD->PDV->CDD
    d = mat_dist[0, idx] + mat_dist[idx, 0]
    return {
        'sequencia': tuple(seq),
        'tempo_desloc_total': t,
        'dist_total': d,
        'dist_deslocamento': d, #toda a distânciia percorrida é distância de deslocamento
        'dist_laco': 0  #não há distância de laço
    }


try:
    df_pdvs = pd.read_csv(caminho_pdvs, sep=';')
    df_dedicadas = pd.read_csv(caminho_dedicadas, sep=';') if os.path.exists(caminho_dedicadas) else pd.DataFrame()
    df_amostra = pd.read_csv(caminho_amostra, sep=';', dtype={'COD PDV': str}).set_index('COD PDV')
    mat_tempo = np.load(caminho_matriz_tempos) /60.0
    mat_dist = np.load(caminho_matriz_distancias) / 1000.0
except Exception as e:
    print(f"Erro ao carregar: {e}")
    exit()


#Dicionários para conversão entre código de PDV e índice na matriz
codigo_indice = {row['COD PDV']: idx for idx, row in df_pdvs.iterrows()}
indice_codigo = {v: k for k, v in codigo_indice.items()}

rotas = {}

# Para cada PDV, cria uma rota exclusiva
for idx, row in df_pdvs.iterrows():
    cod = row['COD PDV']
    if cod == 0: 
        continue  
    #Calcula direto as métricas das rotas
    metricas = calcular_metricas([idx], mat_tempo, mat_dist)
    
    rotas[cod] = {
        'indices': [idx],
        'sequencia': metricas['sequencia'],
        't_desloc': metricas['tempo_desloc_total'],
        'd_total': metricas['dist_total'],
        'd_desloc': metricas['dist_deslocamento'],
        'd_laco': metricas['dist_laco'],
        't_atend': row['tempo_servico_min'],
        'peso': row['peso_total_kg'],
        'vol': row['volume_total_m3'],
        'lata': row['demanda_LATA'],
        'pet': row['demanda_PET'],
        'garrafa': row['demanda_GARRAFA']
    }

print(f"Total: {len(rotas)} rotas individuais")

print("Gerando relatórios")
df_vis = df_pdvs.copy()
df_vis['ROTA_NUMERO'] = 'R0_CDD'
num = 1
for rota_id, rota in rotas.items():
    df_vis.loc[rota['indices'], 'ROTA_NUMERO'] = f"R{num}"
    num += 1
df_vis.to_csv(saida_visualizacao, index=False, sep=';')

relatorio = []
num = 1
for rota_id, r in rotas.items():
    nome = f"R{num}"
    seq_str = " -> ".join([str(indice_codigo.get(i, '??')) for i in r['sequencia']])
    #qtde_pdvs é sempre 1 e tipo_rota é sempre 'Individual'
    relatorio.append({
        'ROTA_NUMERO': nome,
        'tipo_rota': 'Individual',
        'qtde_pdvs': 1,
        'sequencia_pdvs': f"CDD -> {seq_str} -> CDD",
        'peso_total_kg': r['peso'],
        'utilizacao_peso_perc': (r['peso'] / CAPACIDADE_MAXIMA) * 100,
        'volume_total_m3': r['vol'],
        'tempo_total_min': r['t_desloc'] + r['t_atend'],
        'utilizacao_tempo_perc': ((r['t_desloc'] + r['t_atend']) / JORNADA_MAXIMA) * 100,
        'tempo_atendimento_min': r['t_atend'],
        'tempo_deslocamento_min': r['t_desloc'],
        'distancia_total_km': r['d_total'],
        'distancia_deslocamento_km': r['d_desloc'],
        'distancia_laco_km': r['d_laco'],
        'qtde_caixas_LATA': r['lata'],
        'qtde_caixas_PET': r['pet'],
        'qtde_caixas_GARRAFA': r['garrafa']
    })
    num += 1

# Para cada rota dedicada
for idx, ded in df_dedicadas.iterrows():
    nome = f"D{idx + 1}"
    cod = ded['COD PDV']
    i = codigo_indice.get(cod)
    if i is None: 
        continue
    
    t_d = mat_tempo[0, i] + mat_tempo[i, 0]
    d = mat_dist[0, i] + mat_dist[i, 0]
    
    t_s = tempo_atendimento({
        'type': df_amostra.loc[str(cod)]['type'],
        'demanda_LATA': ded['carga_lata'],
        'demanda_PET': ded['carga_pet'],
        'demanda_GARRAFA': ded['carga_garrafa']
    })
    #qtde_pdvs é sempre 1 e tipo_rota é sempre 'Dedicada'
    relatorio.append({
        'ROTA_NUMERO': nome,
        'tipo_rota': 'Dedicada',
        'qtde_pdvs': 1,
        'sequencia_pdvs': f"CDD -> {cod} -> CDD",
        'peso_total_kg': ded['peso_total_caminhao'],
        'utilizacao_peso_perc': (ded['peso_total_caminhao'] / CAPACIDADE_MAXIMA) * 100,
        'volume_total_m3': ded['volume_total_caminhao'],
        'tempo_total_min': t_d + t_s,
        'utilizacao_tempo_perc': ((t_d + t_s) / JORNADA_MAXIMA) * 100,
        'tempo_atendimento_min': t_s,
        'tempo_deslocamento_min': t_d,
        'distancia_total_km': d,
        'distancia_deslocamento_km': d,
        'distancia_laco_km': 0,
        'qtde_caixas_LATA': ded['carga_lata'],
        'qtde_caixas_PET': ded['carga_pet'],
        'qtde_caixas_GARRAFA': ded['carga_garrafa']
    })

# Cria DataFrame com o relatório, ordena e formata números
df_rel = pd.DataFrame(relatorio).sort_values(by='ROTA_NUMERO')
cols_num = df_rel.select_dtypes(include=[np.number]).columns
df_rel[cols_num] = df_rel[cols_num].round(2)
df_rel.to_csv(saida_relatorio, index=False, sep=';')

print(f"Finalizado: {len(rotas)} rotas individuais + {len(df_dedicadas)} dedicadas")