import csv
import json

class Dados:

    def __init__(self, dados):
        self.dados = dados
        self.nome_colunas = self.__get_columns()
        self.quantidade_dados = self.__size_data()

    def __leitura_dados_json(path):
        dados_json = []

        with open(path, "r") as file:
            dados_json = json.load(file)
        return dados_json
    
    def __leitura_dados_csv(path):
        dados_csv = []

        with open(path, "r", encoding="utf-8") as file:
            spamreader = csv.DictReader(file)
            for row in spamreader:
                dados_csv.append(row)
        return dados_csv
    
    @classmethod
    def leitura_tipo_dados(cls, path, tipo_dado):
        dados = []

        if tipo_dado == 'json':
            dados = cls.__leitura_dados_json(path)
        elif tipo_dado == 'csv':
            dados = cls.__leitura_dados_csv(path)
        return cls(dados)
    
    def __get_columns(self):
        return list(self.dados[-1].keys())
    
    def rename_columns(self, key_renamer):
        novos_dados = []

        for old_dados in self.dados:
            dict_temp = {}
            for old_key, value in old_dados.items():
                dict_temp[key_renamer[old_key]] = value
            novos_dados.append(dict_temp)

        self.dados = novos_dados
        self.nome_colunas = self.__get_columns()

    
    def __size_data(self):
        return len(self.dados)
    
    @staticmethod
    def join(dadosA, dadosB):
        dados_fusao = []

        dados_fusao.extend(dadosA.dados)
        dados_fusao.extend(dadosB.dados)

        return Dados(dados_fusao)
    
    def __transformar_dados_fusao_tabela(self):
        dados_fusao_tabela = [self.nome_colunas]

        for row in self.dados:
            linha = []
            for column in self.nome_colunas:
                linha.append(row.get(column, "Indisponivel"))
            dados_fusao_tabela.append(linha)

        return dados_fusao_tabela
    
    def exportar_dados(self, path):
        dados_fusao_tabela = self.__transformar_dados_fusao_tabela()

        with open(path, "w", encoding="utf-8", newline="") as file:
            writer = csv.writer(file, quoting=csv.QUOTE_ALL)
            writer.writerows(dados_fusao_tabela)