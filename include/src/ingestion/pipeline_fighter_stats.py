import json
import os
from asyncio import sleep

from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.chrome.options import Options


# ========================
# Camada de Scraping
# ========================
class Scraper:
    """
    Responsável por acessar páginas web e retornar HTML renderizado.

    Estratégia:
    - Usa Selenium com Chrome headless
    - Simula navegador real (User-Agent)
    - Suporte a ambientes containerizados (Docker/Airflow)

    Observações:
    - Inclui screenshot para debug
    - Timeout configurado para evitar travamentos
    """

    def __init__(self):
        """
        Inicializa o WebDriver com configurações otimizadas para execução em backend.
        """
        options = Options()

        # Configuração para execução headless e ambiente restrito
        options.add_argument("--headless=new")
        options.add_argument("--no-sandbox")
        options.add_argument("--disable-dev-shm-usage")
        options.add_argument("--window-size=1280,720")

        # Simula navegador real (evita bloqueios básicos)
        options.add_argument(
            "user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/114.0.0.0 Safari/537.36"
        )

        self.driver = webdriver.Chrome(options=options)
        self.driver.set_page_load_timeout(30)

    def get_html(self, url):
        """
        Acessa uma página e retorna o HTML renderizado.

        Fluxo:
        1. Abre a URL
        2. Aguarda renderização (JS)
        3. Salva screenshot para debug
        4. Retorna page_source

        Retorna:
            str: HTML ou string vazia em caso de falha
        """
        try:
            self.driver.get(url)

            # Espera simples para carregamento (pode evoluir para WebDriverWait)
            sleep(5)

            # Screenshot para inspeção em caso de erro
            screenshot_path = os.path.join(os.getenv("AIRFLOW_HOME", "/usr/local/airflow"), "include/data/debug.png")
            self.driver.save_screenshot(screenshot_path)

            return self.driver.page_source

        except Exception:
            # Falha silenciosa (pode evoluir para logging estruturado)
            return ""

    def close(self):
        """
        Encerra o driver e libera recursos do sistema.
        """
        self.driver.quit()


# ========================
# Camada de Parsing
# ========================
class TapologyParser:
    """
    Responsável por transformar HTML em dados estruturados.

    Saídas:
    - details: informações gerais do lutador
    - fights: histórico de lutas

    Observações:
    - Baseado na estrutura atual do Tapology
    - Pode quebrar se o HTML mudar (acoplamento estrutural)
    """

    def __init__(self, html):
        """
        Inicializa o parser com o HTML bruto.
        """
        self.soup = BeautifulSoup(html, "html.parser")

    def _clean(self, text):
        """
        Normaliza strings extraídas do HTML.

        Regras:
        - Remove espaços extras, tabs e quebras de linha
        - Remove caracteres indesejados (ex: '|')
        - Retorna 'N/A' para valores vazios
        """
        if not text:
            return "N/A"

        cleaned = " ".join(text.split()).strip()
        return cleaned.replace("|", "").strip()

    def parse_all(self, fighter_name):
        """
        Executa parsing completo da página.

        Etapas:
        1. Extrai detalhes do lutador
        2. Extrai histórico de lutas
        3. Normaliza os dados

        Retorna:
            dict:
                {
                    "details": {...},
                    "fights": [...]
                }
        """
        details = {}

        # ========================
        # Extração de detalhes
        # ========================
        container = self.soup.find("div", id="standardDetails")

        if container:
            for label in container.find_all("strong"):
                key = self._clean(label.get_text()).replace(":", "")

                value_node = label.find_next_sibling(["span", "a"])
                value = self._clean(value_node.get_text()) if value_node else "N/A"

                details[key] = value

        details["Fighter Name"] = self._clean(fighter_name)

        # ========================
        # Extração de lutas
        # ========================
        fights = []

        bouts = self.soup.find_all("div", attrs={"data-bout-id": True})

        for bout in bouts:
            res_tag = bout.select_one(".result .text-white")
            opp_tag = bout.find("a", href=lambda x: x and "/fighters/" in x)
            date_tag = bout.find("span", class_=lambda x: x and "text-tap_3" in x)

            fights.append(
                {
                    "Fighter": self._clean(fighter_name),
                    "Status": self._clean(res_tag.get_text()) if res_tag else "N/A",
                    "Opponent": self._clean(opp_tag.get_text()) if opp_tag else "N/A",
                    "Date": self._clean(date_tag.get_text()) if date_tag else "N/A",
                }
            )

        return {"details": details, "fights": fights}


# ========================
# Camada de Input (Data Source)
# ========================
def load_json_data():
    """
    Carrega dados de entrada (lista de lutadores).

    Origem:
    - Arquivo JSON versionado dentro do projeto

    Observações:
    - Usa AIRFLOW_HOME como base (compatível com execução em container)
    """
    base_path = os.getenv("AIRFLOW_HOME", "/usr/local/airflow")

    path = os.path.join(base_path, "include/src/resources/tapology_ufc_pvp_rkng.json")

    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)
