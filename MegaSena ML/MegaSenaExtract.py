import requests
from bs4 import BeautifulSoup
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

class ExtractFromSite:
    def __init__(self, site):
        self.site = site

    def pg_megasena_info(self):
        response = requests.get(self.site)
        soup = BeautifulSoup(response.content, 'html.parser')
        return soup

class InfoPages:
    @staticmethod
    def url_pages(url_base):
        soup = ExtractFromSite(url_base).pg_megasena_info()
        pg_site_list = soup.find("ul", {'class' : 'pagination'})
        first_page = int(pg_site_list.find("a", text="1").getText())
        last_page = int(pg_site_list.find("a", text="Última").get("href").split("=")[-1])
        return [f"{url_base}?pag={i}" for i in range(first_page, last_page + 1)]

    @staticmethod
    def extract_info(url_page, loc_item):
        data_info = []

        for url in url_page:
            soup = ExtractFromSite(url).pg_megasena_info()
            div_col_md_8 = soup.find("div", {'class': 'col-md-8'})
            strong_tags = div_col_md_8.findAll("strong")
            br = div_col_md_8.find("br")

            if br:
                for tag in strong_tags:
                    next_sibling = tag.next_sibling
                    if next_sibling and isinstance(next_sibling, str):
                        data_info.append(next_sibling.strip().split(" "))

        return [data_info[i][loc_item] if len(data_info[i]) > loc_item else None for i in range(len(data_info))]

    @staticmethod
    def num_concurso(url_page) -> list: # Criando uma lista de concursos que já foram 
        num_concurso = []

        for url in url_page:
            pages = ExtractFromSite(url).pg_megasena_info()
            div_class_col_md_8 = pages.find("div", **{'class':'col-md-8'})
            strong = div_class_col_md_8.findAll("strong")
            for tag in strong:
                num_concurso.append(tag.get_text())

        return [i for i in num_concurso if i != 'Mega da Virada']

    @staticmethod
    def extract_game_sequence(url_page, loc_item):
        numbers = []

        for url in url_page:
            soup = ExtractFromSite(url).pg_megasena_info()
            div_col_md_8 = soup.find("div", {'class': 'col-md-8'})
            spans_dezenas = div_col_md_8.findAll("span", {'class': 'dezenas'})

            for span in spans_dezenas:
                numbers.append(span.get_text())

        games = [numbers[i:i+6] for i in range(0, len(numbers), 6)]
        return [game[loc_item] for game in games]

if __name__ == "__main__":
    urls = InfoPages.url_pages('https://asloterias.com.br/todos-resultados-mega-sena')

    num_concurso = InfoPages.num_concurso(urls)
    data_concurso = InfoPages.extract_info(urls, 1)
    status_ganhador = InfoPages.extract_info(urls, -1)

    game_sequences = [
        InfoPages.extract_game_sequence(urls, i) for i in range(6)
    ]

    df = pd.DataFrame({
        'num_concurso': num_concurso,
        'data_concurso': data_concurso,
        'first': game_sequences[0],
        'second': game_sequences[1],
        'third': game_sequences[2],
        'forth': game_sequences[3],
        'fifth': game_sequences[4],
        'sixth': game_sequences[5],
        'status': status_ganhador
    })

    df.to_parquet('Datasets/MegaSena.pq')