
from db_connector import get_postgres_connection
import requests

def get_consulta(localidade:str, agregado:str, ano:str, variavel:str):
    # ano: 2017 a 2023
    # variaveis: 109|216|214|215
    full_url = f"https://servicodados.ibge.gov.br/api/v3/agregados/{agregado}/periodos/{ano}/variaveis/{variavel}?localidades=N6[{localidade}]&classificacao=81[all]"
    response = requests.get(full_url)

    if response.status_code == 200:
        json_data = response.json()
        return json_data
    else:
        return {}


def teste():
    full_url = "https://servicodados.ibge.gov.br/api/v3/agregados/6874/periodos/2017/variaveis/9572?localidades=N6[N2[1]]&classificacao=796[40597,40598,40599,40600]"
    response = requests.get(full_url)

    if response.status_code == 200:
        json_data = response.json()
        return json_data
    else:
        return {}

def teste_2(localidade:str, ano:str):
    full_url = f"https://servicodados.ibge.gov.br/api/v3/agregados/6874/periodos/{ano}/variaveis/9572?localidades=N6[{localidade}]&classificacao=796[40597,40598,40599,40600]"
    response = requests.get(full_url)

    if response.status_code == 200:
        json_data = response.json()
        return json_data
    else:
        return {}

def get_municipios(agregado:str):    
    url_municipios = f"https://servicodados.ibge.gov.br/api/v3/agregados/{agregado}/localidades/N6"  

    response = requests.get(url_municipios)

    if response.status_code == 200:
        json_data = response.json()
        return json_data
    else:
        return {}
        



if __name__ == "__main__":
    #coleta_producao_agricula_municipal("2510105", "2017")
    #coleta_pesquisa_pecuaria_municipal("2510105", "2017")
    #coleta_producao_agricula_municipal_total("2017")
    #print(get_consulta("2510105", "1612", "2017", "109"))
    #coleta_pesquisa_pecuaria_municipal_full("2017")
    #coleta_censo_agropecuario_total("2017")

    print(teste_2("2510105", "2017"))
    