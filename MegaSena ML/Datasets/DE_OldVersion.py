# pylint: disable=missing-class-docstring
import requests
from bs4 import BeautifulSoup
import pandas as pd 
import pyarrow as pa
import pyarrow.parquet as pq

class ExtractFromSite(): ## BS4 Página Web

    def __init__(self, site : str):
        self.site = site

    def pg_megasena_info(self):  # --> Função para entrar na página
        response = requests.get(self.site)  # --> Requisição a acesso a página
        soup = BeautifulSoup(response.content, 'html.parser')  # --> Extraindo o conteúdo da página
        return soup  # --> Retorna a informação já "parseada" do site


class PageSite(): ### Pegando as informações do total de numeração das páginas

    def __init__(self, soup):
        self.soup = soup 
        self.page = self.paginas()

    def paginas(self): #--> Trazer as informações da páginas que contenham somente a tag : "ul"
        return self.soup.find("ul", {'class' : 'pagination'})
    
    def first_page(self) -> int: #--> Função pega o valor da primeira página

        fst_page = self.page.find("a", text = "1").getText()  #--> Achar a tag "a" com texto informação da primeira página
        return int(fst_page)
    
    def last_page(self) -> int: #--> Função pega o valor da última página

        lst_pg = self.page.find("a", text= "Última").get("href").split("=")[-1] #--> Achar a tag "a" com texto da ultima informação e selecionado e o último item da lista 
        return int(lst_pg)

    def page_list(self): #--> Criando a lista com o range de páginas que contem no site

        fst = self.first_page() #--> Primeira Página
        lst = self.last_page() #--> Última Página
        
        return [str(i) for i in range(fst, lst + 1)] 


class InfoPages(): ### E#xtraindo informações do site
    
    def url_pages(url_base : str) -> list: # Criando uma lista de urls do site 
        soup = ExtractFromSite(url_base).pg_megasena_info()
        pg_site_list = PageSite(soup).page_list()
        return [f"{url_base}?pag={i}" for i in pg_site_list]

    def num_concurso(url_page) -> list: # Criando uma lista de concursos que já foram 
        num_concurso = []

        for url in url_page:
            pages = ExtractFromSite(url).pg_megasena_info()
            div_class_col_md_8 = pages.find("div", **{'class':'col-md-8'})
            strong = div_class_col_md_8.findAll("strong")
            for tag in strong:
                num_concurso.append(tag.get_text())

        return [i for i in num_concurso if i != 'Mega da Virada']

    def info_concurso(url_page : list, loc_item: int) -> list: # Criando uma lista com as datas e status ganhador de cada jogo
        data_info = []

        for url in url_page:
            pages = ExtractFromSite(url).pg_megasena_info()
            div_class_col_md_8 = pages.find("div", **{'class':'col-md-8'})
            strong = div_class_col_md_8.findAll("strong")
            br = div_class_col_md_8.find("br")
            if br:
                for st in strong:
                    next_sibling = st.next_sibling
                    if next_sibling and isinstance(next_sibling, str):  # Verifica se não é None e se é uma string
                        data_info.append(next_sibling.strip().split(" "))

        return [data_info[i][loc_item] for i in range(0,len(data_info))]

    def sequencia_jogos(url_page : list, loc_item: int) -> list:
        numeros = []

        for url in url_page:

            pages = ExtractFromSite(url).pg_megasena_info()
            div_class_col_md_8 = pages.find("div", **{'class':'col-md-8'})
            dezenas = div_class_col_md_8.findAll("span", **{'class': 'dezenas'})

            for i in dezenas:
                numeros.append(i.get_text())
                
        lista_jogos = [numeros[i:i+6] for i in range(0, len(numeros), 6)]
        return [fst[loc_item] for fst in lista_jogos]

class main():

    urls = InfoPages.url_pages('https://asloterias.com.br/todos-resultados-mega-sena')

    num_concurso = InfoPages.num_concurso(urls)
    data_concurso = InfoPages.info_concurso(urls, 1)
    status_ganhador = InfoPages.info_concurso(urls, -1)
    fst_num = InfoPages.sequencia_jogos(urls, 0)
    snd_num = InfoPages.sequencia_jogos(urls, 1)
    thd_num = InfoPages.sequencia_jogos(urls, 2)
    fth_num = InfoPages.sequencia_jogos(urls, 3)
    fif_num = InfoPages.sequencia_jogos(urls, 4)
    six_num = InfoPages.sequencia_jogos(urls, 5)

    df = pd.DataFrame( 
            zip(num_concurso, 
                data_concurso, 
                fst_num, 
                snd_num, 
                thd_num, 
                fth_num, 
                fif_num, 
                six_num, 
                status_ganhador
            ), 
            columns = [
                'num_concurso', 
                'data_concurso', 
                'first', 
                'second', 
                'thrid', 
                'forth', 
                'fifth', 
                'sixth', 
                'status'
            ]
        )

    print(df)


if __name__ == "__main__":
    a = main()
    a.to_parquet('Datasets\MegaSena.pq')





        
    