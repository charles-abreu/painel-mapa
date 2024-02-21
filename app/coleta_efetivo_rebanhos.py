from db_connector import get_postgres_connection
import requests, time

# CONSTANTES
NOME_TABELA = "EFETIVO_REBANHOS"
NOME_COLUNA = "QT_CABECAS"


def coleta_efetivo_rebanho(ano:str, tipo_rebanho:str):
    full_url = f"https://servicodados.ibge.gov.br/api/v3/agregados/3939/periodos/{ano}/variaveis/105?localidades=N6[all]&classificacao=79[{tipo_rebanho}]"
    
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

def coleta_efetivo_rebanho_full(ano:str):
    tipo_rebanho = [
        {"id":"2670", "nome":"Bovino"},
        {"id":"2675", "nome":"Bubalino"},
        {"id":"2672", "nome":"Equino"},
        {"id":"32794", "nome":"Suíno"},
        {"id":"2681", "nome":"Caprino"},
        {"id":"2677", "nome":"Ovino"},
        {"id":"32796", "nome":"Galináceos"},
        {"id":"2680", "nome":"Codornas"}
    ]

    for rebanho in tipo_rebanho:

        print(rebanho["nome"])

        json_resposta = coleta_efetivo_rebanho(ano, rebanho["id"])
        
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
                
                id_registro = id_municipio + "_" + ano + "_" + rebanho["id"]
                # Tenta inserir registro ou, em caso de conflito, apenas atualiza o campo com o valor
                query = f"""
                    INSERT INTO "PAINEL"."S_{NOME_TABELA}" ("ID_{NOME_TABELA}", "CD_MUNICIPIO", "NM_MUNICIPIO", "SG_UF_MUNICIPIO",
                    "CD_CATEGORIA", "NM_CATEGORIA", "AN_ANO", "{NOME_COLUNA}")
                    VALUES('{id_registro}','{id_municipio}', '{nome_municipio}', '{uf_municipio}','{rebanho["id"]}', '{rebanho["nome"]}' ,'{ano}', {valor}) 
                    ON CONFLICT ("ID_{NOME_TABELA}") DO UPDATE SET "{NOME_COLUNA}" = {valor};
                """

                with conn.cursor() as cur:
                    cur.execute(query)
                    conn.commit()

            conn.close()


if __name__ == "__main__":
    for ano in ["2017", "2018", "2019", "2020", "2021", "2022"]:
        print("##########", ano,"############" )
        coleta_efetivo_rebanho_full(ano)

