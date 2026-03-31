import os
from datetime import datetime

import pandas as pd
from airflow.decorators import dag, task

from include.src.ingestion.pipeline_fighter_stats import (
    TapologyParserFighterStats,
    load_json_data,
)
from include.src.utils.load_postgres import PostgresLoader
from include.src.utils.scrapper import Scraper


@dag(
    dag_id="tapology_fighter_data",
    start_date=datetime(2026, 3, 27),
    schedule=None,
    catchup=False,
    tags=["tapology", "fighter", "stats"],
    max_active_tasks=2,
)
def mma_dag():
    """
    Pipeline de ingestão de dados do Tapology.

    Etapas:
    1. Leitura do JSON com lutadores
    2. Scraping das páginas individuais
    3. Parsing dos dados (detalhes + histórico de lutas)
    4. Persistência em CSV (camada Bronze)

    Observações:
    - Usa Dynamic Task Mapping para paralelizar por lutador
    - Estruturado para fácil evolução para Data Lake (Bronze/Silver/Gold)
    """

    # ========================
    # Task: leitura da origem
    # ========================
    @task
    def read_json_task():
        """
        Carrega a lista de lutadores e URLs a partir de um JSON.
        """
        return load_json_data()

    # ========================
    # Task: scraping + parsing
    # ========================
    @task
    def scrape_and_parse_task(fighter_entry):
        """
        Executa coleta e parsing para um lutador.

        Estratégia:
        - Abre navegador headless via Selenium
        - Extrai HTML renderizado
        - Faz parsing com BeautifulSoup
        - Garante fechamento do driver (mesmo com erro)

        Retorna:
            dict estruturado ou None (falha)
        """
        scraper = Scraper()

        try:
            html = scraper.get_html(fighter_entry["url_tapology"])
            if not html:
                return None

            parser = TapologyParserFighterStats(html)
            return parser.parse_all(fighter_entry["fighter"])

        finally:
            # Evita vazamento de recurso (Chrome aberto)
            scraper.close()

    # ========================
    # Task: persistência (Bronze)
    # ========================
    @task
    def save_data_task(all_results):
        """
        Consolida e persiste os dados coletados.

        Regras:
        - Remove resultados inválidos (None)
        - Separa entidades: lutadores e lutas
        - Salva em CSV versionado por data

        Saída:
        - fighters_YYYYMMDD.csv
        - fights_YYYYMMDD.csv
        """
        if not all_results:
            return "Nenhum dado coletado."

        # Filtragem e normalização
        results = [r for r in all_results if r is not None]

        fighters_list = [r["details"] for r in results]
        fights_list = [fight for r in results for fight in r["fights"]]

        base_path = os.getenv("AIRFLOW_HOME", "/usr/local/airflow")
        dtm = datetime.now().strftime("%Y%m%d")

        # Persistência - Fighters
        df_fighters = pd.DataFrame(fighters_list)
        path_fighters = os.path.join(base_path, f"include/data/bronze/fighters.csv")
        os.makedirs(os.path.dirname(path_fighters), exist_ok=True)
        df_fighters.to_csv(path_fighters, index=False)

        # Persistência - Fights
        df_fights = pd.DataFrame(fights_list)
        path_fights = os.path.join(base_path, f"include/data/bronze/fights.csv")
        df_fights.to_csv(path_fights, index=False)

        return f"Arquivos salvos para {len(fighters_list)} lutadores." @ task

    @task
    def load_to_postgres_task(file_paths: dict):
        """
        Consome os caminhos dos CSVs gerados e os carrega no PostgreSQL.
        """
        loader = PostgresLoader(conn_id="postgresql_local")

        # Carrega a tabela de Lutadores (Fighters)
        loader.load_to_db(
            csv_path=file_paths["fighters"], schema="bronze", table_name="fighters"
        )

        # Carrega a tabela de Lutas (Fights)
        loader.load_to_db(
            csv_path=file_paths["fights"], schema="bronze", table_name="fights"
        )

        return "Ingestão na camada Bronze finalizada com sucesso."

    # ========================
    # Orquestração
    # ========================#
    # 1. Extração da lista de lutadores do JSON
    fighters_to_process = read_json_task()

    # 2. Scraping e Parsing (Dynamic Task Mapping com limite de concorrência)
    # Cada tarefa abre um Chrome, processa e retorna um dicionário
    mapped_results = scrape_and_parse_task.expand(fighter_entry=fighters_to_process)

    # 3. Persistência em Sistema de Arquivos (Camada Bronze - Arquivos CSV)
    # Retorna um dicionário com os caminhos dos arquivos gerados: {"fighters": "path/to/csv", "fights": "path/to/csv"}
    file_paths = save_data_task(mapped_results)

    # 4. Ingestão no PostgreSQL (Camada Bronze - Tabelas SQL)
    # Envia os arquivos salvos para o schema 'bronze' do banco 'tapologymapper'
    load_to_postgres_task(file_paths)
    # load_to_postgres_task(
    #     {
    #         "fighters": "/usr/local/airflow/include/data/bronze/fighters.csv",
    #         "fights": "/usr/local/airflow/include/data/bronze/fights.csv",
    #     }
    # )


# Instancia a DAG
mma_dag()
