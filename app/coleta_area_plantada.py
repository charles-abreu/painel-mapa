from db_connector import get_postgres_connection
import requests, time

# CONSTANTES
NOME_TABELA = "AREA_PLANTADA"
NOME_COLUNA = "QT_HECTARES"


def coleta_area_plantada(ano:str, cod_uf:str):

    categorias = "40129,40092,45982,40329,40130,40099,40101,40102,40103,40131,40136,40104,40105,40137,40468,40138,40139,40140,40141,40330,40106,40331,40142,40143,40107,40108,40109,40144,40145,40146,40147,40110,40111,40112,40148,40113,40114,40149,40150,40115,40151,40152,40116,40260,40117,40261,40118,40119,40262,40263,40264,40120,40121,40122,40265,40266,40267,40268,40269,40123,40270,40124,40125,40271,40126,40127,40128,40272,40273,40274"
    full_url = f"https://servicodados.ibge.gov.br/api/v3/agregados/5457/periodos/{ano}/variaveis/8331%7C216%7C214%7C112%7C215?localidades=N6[N3[{cod_uf}]]&classificacao=782[{categorias}]"
    
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

def coleta_area_plantada_full(ano:str, cod_uf:str):
    
    json_resposta = coleta_area_plantada(ano, str(cod_uf))

    if json_resposta:
        # conexão com o banco
        conn =  get_postgres_connection()

        for resultado in json_resposta[0]["resultados"]:
            id_categoria = list(resultado["classificacoes"][0]["categoria"].keys())[0]
            nome_categoria = list(resultado["classificacoes"][0]["categoria"].values())[0]

            for local in resultado["series"]:
                id_municipio = local["localidade"]["id"]
                nome_municipio = local["localidade"]["nome"].split(" - ")[0].strip().replace("'", " ")
                uf_municipio = local["localidade"]["nome"].split(" - ")[1].strip()
                valor = list(local["serie"].values())[0]

                try:
                    valor = int(valor)
                except  ValueError:
                    valor = "cast(null as INT)"
                
                id_registro = id_municipio + "_" + ano + "_" + id_categoria
                # Tenta inserir registro ou, em caso de conflito, apenas atualiza o campo com o valor
                query = f"""
                    INSERT INTO "PAINEL"."S_{NOME_TABELA}" ("ID_{NOME_TABELA}", "CD_MUNICIPIO", "NM_MUNICIPIO", "SG_UF_MUNICIPIO",
                    "CD_CATEGORIA", "NM_CATEGORIA", "AN_ANO", "{NOME_COLUNA}")
                    VALUES('{id_registro}','{id_municipio}', '{nome_municipio}', '{uf_municipio}','{id_categoria}', '{nome_categoria}' ,'{ano}', {valor}) 
                    ON CONFLICT ("ID_{NOME_TABELA}") DO UPDATE SET "{NOME_COLUNA}" = {valor};
                """

                with conn.cursor() as cur:
                    cur.execute(query)
                    conn.commit()

if __name__ == "__main__":
    ufs = [11,12,13,14,15,16,17,21,22,23,24,25,26,27,28,29,31,32,33,35,41,42,43,50,51,52,53]
    anos = ["2017", "2018", "2019", "2020", "2021", "2022"]

    for ano in anos:
        print(ano, ":", end=' ')
        for uf in ufs:
            print(uf, end=',')
            coleta_area_plantada_full(ano, str(uf))
        
        print("\n")
