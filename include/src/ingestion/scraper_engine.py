import logging
from time import sleep

from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait


class ScraperEngine:
    def __init__(self):
        options = Options()
        options.add_argument("--headless")
        options.add_argument("--no-sandbox")
        options.add_argument("--disable-dev-shm-usage")

        # Caminho padrão do chromium-driver no Debian/Astro
        service = Service(executable_path="/usr/bin/chromedriver")

        self.driver = webdriver.Chrome(service=service, options=options)
        self.wait = WebDriverWait(self.driver, 20)

    def get_html(self, url: str) -> str:
        try:
            self.driver.get(url)
            self.wait.until(EC.presence_of_element_located((By.TAG_NAME, "body")))
            sleep(2)
            return self.driver.page_source
        except Exception as e:
            logging.error(f"Erro ao acessar {url}: {e}")
            return ""

    def close(self):
        self.driver.quit()
