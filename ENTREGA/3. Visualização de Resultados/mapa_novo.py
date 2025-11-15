import pandas as pd
import folium
import requests
from itertools import cycle

# Define a chave da API do Google Maps
GOOGLE_API_KEY = 'KEY retirada por motivos de segurança'

# CAMINHOS ENTRADA
caminho_relatorio = "ENTREGA/0. DADOS/rotas/hibrido/VERSAOFINAL/relatorio_geral_rotas.csv"
caminho_visualizacao = "ENTREGA/0. DADOS/rotas/hibrido/VERSAOFINAL/rotas_clusterizadas_visualizacao.csv"
#CAMINHOS SAÍDA
saida_mapa = "ENTREGA/4_visualização_resultados/mapa_rotas_FINAL.html"

df_rotas = pd.read_csv(caminho_relatorio, sep=";")
df_pontos = pd.read_csv(caminho_visualizacao, sep=";")

# Identificação do CDD (Centro de Distribuição)
cdd = df_pontos[df_pontos["name"].str.upper() == "CDD"].iloc[0]
cdd_coords = (cdd["latitude"], cdd["longitude"])

# Cria dicionário para mapear código do PDV para suas coordenadas
coord_por_pdv = df_pontos.set_index("COD PDV")[["latitude", "longitude"]].to_dict("index")

#Mapa base centrado no CDD com zoom 12
mapa = folium.Map(location=cdd_coords, zoom_start=12, tiles="OpenStreetMap")

# Define paleta de cores para diferenciar as rotas
cores_rotas = [
    "#FF0000", "#0000FF", "#008000", "#FFD700", 
    "#FF00FF", "#00CED1", "#FF8C00", "#800000", 
    "#4B0082", "#A52A2A", "#2E8B57", "#FF1493"  
]
ciclo_cores = cycle(cores_rotas)

# Marker para CDD, casinha vermelha
folium.Marker(
    location=cdd_coords,
    popup="CDD",
    icon=folium.Icon(color="red", icon="home", prefix="fa"), 
).add_to(mapa)

# Função que obtém a rota real do Google Maps para uma sequência de PDVs
def gerar_rota_google(sequencia_pdvs):
    url = "https://maps.googleapis.com/maps/api/directions/json"
    
    # Define origem e destino como o CDD (rota circular)
    origem = f"{cdd_coords[0]},{cdd_coords[1]}"
    destino = origem
    # Monta string de waypoints (pontos intermediários) com os PDVs da rota
    waypoints = "|".join([f"{coord_por_pdv[int(p)]['latitude']},{coord_por_pdv[int(p)]['longitude']}" 
                          for p in sequencia_pdvs])
    
    # Parâmetros da requisição à API Directions
    params = {
        "origin": origem,
        "destination": destino,
        "waypoints": waypoints,
        "key": GOOGLE_API_KEY,
        "language": "pt-BR"
    }
    # Requisição à API e verifica status
    resp = requests.get(url, params=params)
    data = resp.json()
    if data["status"] != "OK":
        print(f"Erro API: {data['status']} ({len(sequencia_pdvs)} PDVs)")
        return []
    polyline = data["routes"][0]["overview_polyline"]["points"]
    
    # Decodificação da polyline do google
    def decodificar_polyline(encoded):
        pontos = []
        idx = 0
        lat, lng = 0, 0
        # Percorre a string codificada
        while idx < len(encoded):
            result, shift = 0, 0
            # Decodifica latitude
            while True:
                b = ord(encoded[idx]) - 63  #Converte caractere para número
                idx += 1
                result |= (b & 0x1F) << shift  # Operação bit a bit
                shift += 5
                if b < 0x20:  # Se bit 6 está desligado, terminou
                    break
            dlat = ~(result >> 1) if result & 1 else (result >> 1)  # Decodifica delta
            lat += dlat  # Adiciona delta à latitude acumulada
            result, shift = 0, 0
            #Decodifica longitude (mesmo processo)
            while True:
                b = ord(encoded[idx]) - 63
                idx += 1
                result |= (b & 0x1F) << shift
                shift += 5
                if b < 0x20:
                    break
            dlng = ~(result >> 1) if result & 1 else (result >> 1)
            lng += dlng 
            # Converte de inteiro para coordenada decimal
            pontos.append((lat / 1e5, lng / 1e5))
        return pontos
    
    # Retorna lista de pontos decodificados
    return decodificar_polyline(polyline)

#Para cada rota no relatório, adiciona ao mapa
for _, rota in df_rotas.iterrows():
    rota_id = rota["ROTA_NUMERO"]
    #Extrai a sequência de pdvs da rota
    seq_str = str(rota["sequencia_pdvs"]).replace("CDD", "")
    seq_pdvs = [s.strip() for s in seq_str.replace("->", ",").split(",") if s.strip()]
    
    if not seq_pdvs:
        continue
    
    # Seleciona próxima cor do ciclo
    cor = next(ciclo_cores)
    print(f"Processando {rota_id}: {len(seq_pdvs)} PDVs")
    
    # Obtém pontos da rota real do Google Maps
    pontos_linha = gerar_rota_google(seq_pdvs)
    
    #1 layer por rota, facilita visualização
    camada_rota = folium.FeatureGroup(name=f"{rota_id}")
    
    #Detalhes sobre a rota no mapa
    folium.PolyLine(
        locations=pontos_linha,
        color=cor,
        weight=5, 
        opacity=0.8, 
        tooltip=f"{rota_id}" 
    ).add_to(camada_rota)
    
    # Para cada PDV na sequência, adiciona um marcador numerado,conforme sua posição
    for idx, pdv in enumerate(seq_pdvs, start=1):  
        pdv = int(pdv)
        if pdv not in coord_por_pdv: 
            continue     

        lat = coord_por_pdv[pdv]["latitude"]
        lon = coord_por_pdv[pdv]["longitude"]
        
        # Cria HTML para o popup, janela informativa por ponto
        popup_html = f"""
        <b>COD PDV:</b> {pdv}<br>
        <b>Rota:</b> {rota_id}<br>
        <b>Ordem:</b> {idx}
        """
        #Marcador customizado (círculo numerado)
        folium.map.Marker(
            [lat, lon],
            popup=folium.Popup(popup_html, max_width=250),
            icon=folium.DivIcon(
                html=f"""
                <div style="
                    font-size: 11pt;
                    color: white;
                    background-color: {cor};
                    border-radius: 50%;
                    width: 28px;
                    height: 28px;
                    text-align: center;
                    line-height: 28px;
                    border: 2px solid black;
                    ">
                    {idx}
                </div>
                """
            ),
        ).add_to(camada_rota)
    
    camada_rota.add_to(mapa)

#Adiciona controle de camadas,permitindo ligar/desligar rotas individualmente
folium.LayerControl(collapsed=False).add_to(mapa)
# Salva o mapa em arquivo HTML
mapa.save(saida_mapa)
print(f"\nMapa gerado: {saida_mapa}")