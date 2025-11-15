import pandas as pd
import numpy as np
import os
import time
from ortools.constraint_solver import routing_enums_pb2
from ortools.constraint_solver import pywrapcp

# RESTRIÇÕES
CAPACIDADE_MAXIMA = 12000.0
JORNADA_MAXIMA = 510.0

# CAMINHOS DE ENTRADA
caminho_pdvs = 'ENTREGA/0. DADOS/rotas/hibrido/preclusterizacao/pdvs_para_clusterizar.csv'
caminho_dedicadas = 'ENTREGA/0. DADOS/rotas/hibrido/preclusterizacao/rotas_dedicadas_excesso.csv'
caminho_savings = 'ENTREGA/0. DADOS/matrizes_amostra/csv/savings_list_ranked.csv'
caminho_matriz_tempos = 'ENTREGA/0. DADOS/matrizes_amostra/npy/matriz_tempos.npy'
caminho_matriz_distancias = 'ENTREGA/0. DADOS/matrizes_amostra/npy/matriz_distancias.npy'
caminho_amostra = 'ENTREGA/0. DADOS/amostra/estabelecimentos_bh_amostra_bairros.csv'

# CAMINHOS DE SAÍDA
saida_visualizacao = 'ENTREGA/0. DADOS/rotas/hibrido/versao4/rotas_clusterizadas_visualizacao2.csv'
saida_relatorio = 'ENTREGA/0. DADOS/rotas/hibrido/versao4/relatorio_geral_rotas2.csv'

# Tempo de atendimento de acordo com o tipo de estabelecimento e a demanda
def tempo_atendimento(dados_pdv):
    if dados_pdv.get('type') == 'CDD':
        return 0
    
    tempo_base = 14.0
    tempo_fila = 60.0 if dados_pdv.get('type') in ['supermarket', 'alcohol'] else 0
    cx = dados_pdv.get('demanda_LATA', 0) + dados_pdv.get('demanda_PET', 0) + dados_pdv.get('demanda_GARRAFA', 0)
    tempo_desc = (cx * 20.0) / 60.0
    tempo_ret = (dados_pdv.get('demanda_GARRAFA', 0) * 20.0) / 60.0
    
    return round(tempo_base + tempo_fila + tempo_desc + tempo_ret, 2)

#Métricas de tempo e distância para a sequência de visitas)
def calcular_metricas(seq, mat_tempo, mat_dist):
    if not seq:
        return {'sequencia': (), 'tempo_desloc_total': 0, 'dist_total': 0, 
                'dist_deslocamento': 0, 'dist_laco': 0}
    
    t = mat_tempo[0, seq[0]]
    d = mat_dist[0, seq[0]]
    laco = 0
    
    for i in range(len(seq) - 1):
        t += mat_tempo[seq[i], seq[i+1]]
        trecho = mat_dist[seq[i], seq[i+1]]
        laco += trecho
        d += trecho
    
    t += mat_tempo[seq[-1], 0]
    d += mat_dist[seq[-1], 0]
    desloc = mat_dist[0, seq[0]] + mat_dist[seq[-1], 0]
    
    return {'sequencia': tuple(seq), 'tempo_desloc_total': t, 'dist_total': d,
            'dist_deslocamento': desloc, 'dist_laco': laco}

#Esta versão usa OR-Tools para resolver o TSP
def resolver_tsp(indices, mat_tempo, mat_dist):
    # Se houver apenas um PDV, calcula métricas diretamente
    if len(indices) == 1:
        return calcular_metricas(indices, mat_tempo, mat_dist)
    
    # Cria mapa incluindo o CDD (índice 0) e os PDVs
    mapa = [0] + indices
    n = len(mapa)
    
    # Cria matriz nxn de custos baseada em tempo
    custo = np.zeros((n, n))
    for i in range(n):
        for j in range(n):
            custo[i, j] = mat_tempo[mapa[i], mapa[j]]
    
    #Configuração do OR-Tools
    mgr = pywrapcp.RoutingIndexManager(n, 1, 0)  # n nós, 1 veículo, começa em 0
    routing = pywrapcp.RoutingModel(mgr)
    
    # Define função de callback para custos de transição
    def callback(from_idx, to_idx):
        from_node = mgr.IndexToNode(from_idx)
        to_node = mgr.IndexToNode(to_idx)
        return int(custo[from_node, to_node] * 1000)  # Multiplica por 1000 para usar inteiros
    
    # Registra o callback no modelo
    transit_idx = routing.RegisterTransitCallback(callback)
    routing.SetArcCostEvaluatorOfAllVehicles(transit_idx)
    
    #Definição de parametros de busca e limite de 1 segundo
    params = pywrapcp.DefaultRoutingSearchParameters()
    params.first_solution_strategy = routing_enums_pb2.FirstSolutionStrategy.PATH_CHEAPEST_ARC
    params.time_limit.seconds = 1 
    #Armazena a sequencia da solução encontrada, caso haja. Se não, retorna infinito 
    sol = routing.SolveWithParameters(params)
    if not sol:
        return {'tempo_desloc_total': float('inf')}
    seq = []
    idx = routing.Start(0)
    idx = sol.Value(routing.NextVar(idx))
    while not routing.IsEnd(idx):
        node = mgr.IndexToNode(idx)
        seq.append(mapa[node])
        idx = sol.Value(routing.NextVar(idx))

    return calcular_metricas(seq, mat_tempo, mat_dist)

try:
    df_pdvs = pd.read_csv(caminho_pdvs, sep=';')
    df_savings = pd.read_csv(caminho_savings, sep=';')
    df_dedicadas = pd.read_csv(caminho_dedicadas, sep=';') if os.path.exists(caminho_dedicadas) else pd.DataFrame()
    df_amostra = pd.read_csv(caminho_amostra, sep=';', dtype={'COD PDV': str}).set_index('COD PDV')
    mat_tempo = np.load(caminho_matriz_tempos) / 60.0
    mat_dist = np.load(caminho_matriz_distancias) /1000.0
except Exception as e:
    print(f"Erro: {e}")
    exit()

#Dicionários para conversão entre código de PDV e índice na matriz
codigo_indice = {row['COD PDV']: idx for idx, row in df_pdvs.iterrows()}
indice_codigo = {v: k for k, v in codigo_indice.items()}

print(f"Inicializando {len(df_pdvs) - 1} rotas")
rotas = {}
pdv_para_rota = {}

# Para cada PDV, cria uma rota exclusiva e calcula suas métricas
for idx, row in df_pdvs.iterrows():
    cod = row['COD PDV']
    if cod == 0:
        continue
    #Usa a função de resolvedor tsp OR-TOOLS
    stats = resolver_tsp([idx], mat_tempo, mat_dist)
    
    rotas[cod] = {
        'indices': [idx],
        'sequencia': stats['sequencia'],
        't_desloc': stats['tempo_desloc_total'],
        'd_total': stats['dist_total'],
        'd_desloc': stats['dist_deslocamento'],
        'd_laco': stats['dist_laco'],
        't_atend': row['tempo_servico_min'],
        'peso': row['peso_total_kg'],
        'vol': row['volume_total_m3'],
        'lata': row['demanda_LATA'],
        'pet': row['demanda_PET'],
        'garrafa': row['demanda_GARRAFA']
    }
    pdv_para_rota[cod] = cod

#FASE 2: CLUSTERIZAÇÃO (e OTIMIZAÇÃO integrada)
#Nessa versão, a fase de Clusterização e Otimização estão intergradas, utilizando o TSP OR-TOOLS
inicio = time.time()
fusoes = 0

# Percorre a lista de savings em ordem decrescente
for idx, sav in df_savings.iterrows():
    #Para observação da evolução do processo de clusterização
    if idx % 250 == 0:
        print(f"Processando {idx}/{len(df_savings)}")
    
    cod_i = sav['COD_PDV_Origem']
    cod_j = sav['COD_PDV_Destino']
    rota_i = pdv_para_rota.get(cod_i)
    rota_j = pdv_para_rota.get(cod_j)

    #Verifica se ambos os PDVs existem e estão em rotas diferentes
    if rota_i is not None and rota_j is not None and rota_i != rota_j:
        obj_i = rotas[rota_i]
        obj_j = rotas[rota_j]
        # Verifica restrição de peso
        peso_novo = obj_i['peso'] + obj_j['peso']
        if peso_novo <= CAPACIDADE_MAXIMA:
            # Verifica o tempo de atendimento            
            t_atend = obj_i['t_atend'] + obj_j['t_atend']
            if t_atend <= JORNADA_MAXIMA:
                novos = obj_i['indices'] + obj_j['indices']
                #Solver OR-TOOLS para cada tentativa de fusão
                stats = resolver_tsp(novos, mat_tempo, mat_dist)
                t_total = t_atend + stats['tempo_desloc_total']
                #Verifica se a rota combinada é viável
                if t_total <= JORNADA_MAXIMA:
                    fusoes += 1
                    #Atualiza a rota_i com os dados combinados dos pdvs
                    rotas[rota_i] = {
                        'indices': novos,
                        'sequencia': stats['sequencia'],
                        't_desloc': stats['tempo_desloc_total'],
                        'd_total': stats['dist_total'],
                        'd_desloc': stats['dist_deslocamento'],
                        'd_laco': stats['dist_laco'],
                        't_atend': t_atend,
                        'peso': peso_novo,
                        'vol': obj_i['vol'] + obj_j['vol'],
                        'lata': obj_i['lata'] + obj_j['lata'],
                        'pet': obj_i['pet'] + obj_j['pet'],
                        'garrafa': obj_i['garrafa'] + obj_j['garrafa']
                    }
                    # Atualiza o mapeamento de PDV para rota
                    for i in obj_j['indices']:
                        pdv_para_rota[indice_codigo[i]] = rota_i
                    del rotas[rota_j]

fim = time.time()
print(f"\nConcluído: {fusoes} fusões, {len(rotas)} rotas ({fim - inicio:.1f}s)")

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
#Para cada rota clusterizada:
for rota_id, r in rotas.items():
    nome = f"R{num}"
    #Cria a string que ordena a sequência de pdvs no cluster
    seq_str = " -> ".join([str(indice_codigo.get(i, '??')) for i in r['sequencia']])
    #qtde_pdvs varia de acordo com a rota. tipo_rota é clusterizada
    relatorio.append({
        'ROTA_NUMERO': nome,
        'tipo_rota': 'Clusterizada',
        'qtde_pdvs': len(r['indices']),
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

#Para cada rota dedicada:
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

print("Finalizado")