from db_connector import get_postgres_connection
import requests, time

# CONSTANTES
NOME_TABELA = "ESTABELECIMENTOS_AGROPECUARIOS"
NOME_COLUNA = "QT_MAQUINAS"


def coleta_estabelecimentos_agropecuarios(ano:str, cod_categoria:str, cod_regiao:str):
    full_url = f"https://servicodados.ibge.gov.br/api/v3/agregados/6874/periodos/{ano}/variaveis/9572?localidades=N6[N2[{cod_regiao}]]&classificacao=796[{cod_categoria}]"
    
    tentativas = 0
    # Realiza tentativas em caso de falhas na requisição
    while tentativas < 10:
        try:
            response = requests.get(full_url)
        except:
            tentativas += 1
            print("Erro na requisição!")
            time.sleep(5)
        else:
            break

    if response.status_code == 200:
        json_data = response.json()
        return json_data
    else:
        return {}


def coleta_estabelecimentos_agropecuarios_full(ano:str, cod_regiao:str):

        print(ano, "-", cod_regiao)

        tipo_categoria=[
            {"id":"40597", "nome":"Tratores"},
            {"id":"40598", "nome":"Semeadeiras/plantadeiras"},
            {"id":"40599", "nome":"Colheitadeiras"},
            {"id":"40600", "nome":"Adubadeiras e/ou distribuidoras de calcário"}
        ]

        for categoria in tipo_categoria:
            
            json_resposta = coleta_estabelecimentos_agropecuarios(ano, categoria["id"], cod_regiao)

            if json_resposta:
                localidades = json_resposta[0]["resultados"][0]["series"]
                conn =  get_postgres_connection()

                for local in localidades:
                    id_municipio = local["localidade"]["id"]
                    nome_municipio = local["localidade"]["nome"].split(" - ")[0].strip().replace("'", " ")
                    uf_municipio = local["localidade"]["nome"].split(" - ")[1].strip()
                    valor = list(local["serie"].values())[0]

                    try:
                        valor = int(valor)
                    except  ValueError:
                        valor = "cast(null as INT)"

                    id_registro = id_municipio + "_" + ano + "_" + categoria["id"]

                    # Tenta inserir registro ou, em caso de conflito, apenas atualiza o campo com o valor
                    query = f"""
                        INSERT INTO "PAINEL"."S_{NOME_TABELA}" ("ID_{NOME_TABELA}", "CD_MUNICIPIO", "NM_MUNICIPIO", "SG_UF_MUNICIPIO",
                        "CD_CATEGORIA", "NM_CATEGORIA", "AN_ANO", "{NOME_COLUNA}")
                        VALUES('{id_registro}','{id_municipio}', '{nome_municipio}', '{uf_municipio}', '{categoria["id"]}', '{categoria["nome"]}', '{ano}', {valor}) 
                        ON CONFLICT ("ID_{NOME_TABELA}") DO UPDATE SET "{NOME_COLUNA}" = {valor};
                    """

                    with conn.cursor() as cur:
                        cur.execute(query)
                        conn.commit()
                
                conn.close()

if __name__ == "__main__":

    for cod_regiao in [1, 2, 3, 4, 5]:
        coleta_estabelecimentos_agropecuarios_full("2017", str(cod_regiao))