# Algoritmo para consulta à Places API na Google Cloud Plattaform.
# O procedimento baseia-se em pares de coordenadas centrais junto ao um raio de busca para retornar
# lugares registrados na base de dados da API.
#
# Autor: Felipe de Medeiros Costa - https://github.com/felipedmc | https://www.linkedin.com/in/felipedmcosta/
#
import googlemaps as gmp
import json
import pandas as pd
import time

# Sua chave de API da GCP (substitua "CHAVE_API" pela chave fornecida à vocÊ pela GCP)
api_key = 'CHAVE_API'

# Raio de busca em metros (ajuste conforme necessário)
raio = 100  

# Criação de objeto Client do mósulo googlemaps para operar na API
gmpClient= gmp.Client(api_key)

# Aquisição de pares de coornedadas a partir de arquivo excel (o módulo pandas permite adquirir esse dados de outros formatos de arquivo)
pontos_grid= pd.read_excel('grid_to_points.xls')
pontos_grid= pontos_grid.loc[:,['LAT', 'LONG']]
pontos_grid= pontos_grid.reset_index()

# Inicializando variáveis utilizadas para operar as iterações e formatar arquivo final com os resultados
lista_lugares= []
proxima_pagina= None
lugar_id_list= []

# Iteração por cada par de coordenadas adquiridas
for i in range(len(pontos_grid)):
    coordenadas= tuple(pontos_grid.loc[i,['LAT','LONG']])
    
    # A Places API tem problemas com muitas requisições rápidas, trazendo resultados duplicados ou erros.
    # Há necessidade de forçar uma espera de 2 a 3 segundos para garantir resposta adequada da GCP.
    time.sleep(3)
    
    # Atribuição de resultados ao objeto "response"
    response= gmpClient.places_nearby(coordenadas, radius= raio, page_token= proxima_pagina)
    
    #Checkagem de resultado da requisição ao servidor
    if response['status'] == 'OK':
        # print(f'Ponto {i} OK')
        for lugar in response['results']:
            lugar_id= lugar['place_id']
            
            #Verificação de duplicatas e conformação da lista de lugares À ser escrita em arquivo posteriormente
            if lugar_id not in lugar_id_list:
                lugar_id_list.append(lugar['place_id'])
                dados_lugar= {
                    'ID': lugar['place_id'],
                    'NOME': lugar['name'],
                    'LAT': lugar['geometry']['location']['lat'],
                    'LON': lugar['geometry']['location']['lng'],
                    'TIPO': lugar.get('types')[0]
                }
                lista_lugares.append(dados_lugar)
        
        # A PlacesAPI retorna os dados em páginas de 20 registros, com no máximo 3 páginas por resposta, ou seja, 60 registros por requisição.
        # Ressalta-se a importância de malha de coordenadas e raio de busca adequado à densidade de pontos nas áreas de estudo.
        # O loop é reiniciado com a nova página da resposta, caso exista token para uma no objeto "response".
        if 'next_page_token' in response.keys():
            proxima_pagina= response['next_page_token']
        else:
            proxima_pagina= None
    
    # Caso o status da requisição seja diferente de "OK" no objeto "response"
    else:
        print(f'Erro na solicitação à API no Ponto {i}: {response["status"]}')

# conformando arquivo final em extensões json e csv (outros formatos de tabela são possíveis através do módulo pandas).                     
if lista_lugares:
    
    # Inserir nome do arquivo json desejado em "NOME_ARQUIVO_JSON" (com final ".json")
    with open('NOME_ARQUIVO_JSON.json', 'w', encoding= 'utf8') as lugares:
        json.dump(lista_lugares, lugares, indent= 4, ensure_ascii= False )
    
    # conformando objeto Dataframe do módulo pandas, e o armazenando em arquivo csv (substituir "NOME_ARQUIVO_CSV" pelo nome desejado com final ".csv")
    df_lugares= pd.DataFrame(lista_lugares)
    df_lugares.to_csv('NOME_ARQUIVO_CSV.csv', sep= ';')