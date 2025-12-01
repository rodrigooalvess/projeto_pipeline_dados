from etl_pipeline_dados import Dados

# aqui voce deve passar o caminho onde estao os dados da empresa que voce deseja fundir
path_dados_empresa_A = "data_raw/dados_empresaA.json"
path_dados_empresa_B = "data_raw/dados_empresaB.csv"

# Extract

dados_empresa_A = Dados.leitura_tipo_dados(path_dados_empresa_A, "json")
print(f"Colunas de Dados da Empresa A: {dados_empresa_A.nome_colunas}")
print(f"Quantidade de Dados da Empresa A: {dados_empresa_A.quantidade_dados} Dados")

dados_empresa_B = Dados.leitura_tipo_dados(path_dados_empresa_B, "csv")
print(f"Colunas de Dados da Empresa B: {dados_empresa_B.nome_colunas}")
print(f"Quantidade de Dados da Empresa B: {dados_empresa_B.quantidade_dados} Dados")

# Trasnform 

# essa etapa eh pra transformar as colunas das duas empresas e deixá-las com as mesmas chaves de dict ou mesmo nome
key_renamer = {
    "Nome do Item": "Nome do Produto",
    "Classificação do Produto": "Categoria do Produto",
    "Valor em Reais (R$)": "Preço do Produto (R$)",
    "Quantidade em Estoque": "Quantidade em Estoque",
    "Nome da Loja": "Filial",
    "Data da Venda": "Data da Venda"
# "CHAVE QUE QUER ALTERAR": "VALOR NOVO PARA CHAVE"
}

dados_empresa_B.rename_columns(key_renamer)
print(f"Colunas de Dados da Empresa B Renomeadas Para: {dados_empresa_B.nome_colunas}")

dados_fusao = Dados.join(dados_empresa_A, dados_empresa_B)
print("A Fusão das Empresas foi Realizada com Sucesso!")
print(f"Colunas de Dados da Fusão: {dados_fusao.nome_colunas}")
print(f"Quantidade de Dados Fundidos: {dados_fusao.quantidade_dados} Dados")

# Load

path_dados_fusao = "data_processed/dados_fusao.csv"
dados_fusao.exportar_dados(path_dados_fusao)
print(f"{dados_fusao.quantidade_dados} Dados Exportados Para: {path_dados_fusao}")

