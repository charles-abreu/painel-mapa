from db_connector import get_postgres_connection
import requests, time

def coleta_pesquisa_pecuaria_municipal(id_municipio:str, ano:str):    
    full_url = f"https://servicodados.ibge.gov.br/api/v3/agregados/3939/periodos/{ano}/variaveis/105?localidades=N6[{id_municipio}]&classificacao=79[all]"
    
    tentativas = 0

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
        json_resp = response.json()
    else:
        json_resp = {}
    
    #print(json_resp)

    if json_resp:
        # Conexão com o posgres
        conn =  get_postgres_connection()
        for variavel_resp in json_resp:
            # A depender da variavel será armazenada em uma coluna diferente na tabela.
            nome_coluna = ""

            if variavel_resp["id"] == "105":
                nome_coluna = "nm_efetivo_rebanho"
            else:
                continue

            for resultado in variavel_resp["resultados"]:
                id_categoria = list(resultado["classificacoes"][0]["categoria"].keys())[0]
                nome_categoria = list(resultado["classificacoes"][0]["categoria"].values())[0]
                nome_municipio = resultado["series"][0]["localidade"]["nome"].split("(")[0].strip().replace("'", "")
                uf_municipio = resultado["series"][0]["localidade"]["nome"].split("(")[1].replace(")", "").strip()
                valor = list(resultado["series"][0]["serie"].values())[0]

                try:
                    valor = int(valor)
                except  ValueError:
                    valor = "cast(null as INT)"

                id = id_municipio + "_" + ano + "_" + id_categoria

                query = f"""
                    INSERT INTO mapa.s_pesquisa_pecuaria_municipal (cd_id, cd_municipio, nm_municipio, cd_uf_municipio,
                        cd_categoria, nm_categoria, an_ano, {nome_coluna})
                    VALUES('{id}','{id_municipio}', '{nome_municipio}', '{uf_municipio}', '{id_categoria}', '{nome_categoria}', 
                    '{ano}', {valor}) 
                    ON CONFLICT (cd_id) 
                    DO 
                    UPDATE SET {nome_coluna} = {valor};
                """

                with conn.cursor() as cur:
                    cur.execute(query)
                    conn.commit()

        conn.close()

def coleta_pesquisa_pecuaria_municipal_full(ano:str):
    agregado = "3939"

    # Listando municípios
    url_municipios = f"https://servicodados.ibge.gov.br/api/v3/agregados/{agregado}/localidades/N6"  
    response = requests.get(url_municipios)

    if response.status_code == 200:
        municipios_list = response.json()
        print("Municipios listados com sucesso!")
        
        for i, municipio in enumerate(municipios_list):
            if i % 10 == 0:
                print(i, end=", ")

            id_municipio = municipio["id"]
            coleta_pesquisa_pecuaria_municipal(id_municipio, ano)
    else:
        print("Erro ao listar municípios!")
