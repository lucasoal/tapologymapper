import os
from asyncio import sleep

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
            screenshot_path = os.path.join(
                os.getenv("AIRFLOW_HOME", "/usr/local/airflow"),
                "include/data/debug.png",
            )
            self.driver.save_screenshot(screenshot_path)

            return self.driver.page_source

        except Exception as e:
            # Falha silenciosa (pode evoluir para logging estruturado)
            return e

    def close(self):
        """
        Encerra o driver e libera recursos do sistema.
        """
        self.driver.quit()
