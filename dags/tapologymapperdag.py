import os
from datetime import datetime

import pandas as pd
from airflow.decorators import dag, task

# Importando as funções puras do seu include
from include.src.ingestion.run_pipeline import get_fighters_list, scrape_single_fighter


@dag(start_date=datetime(2026, 3, 26), schedule=None, catchup=False, tags=["mma", "medallion", "poo"])
def mma_modern_pipeline():

    @task
    def list_fighters_task():
        # Não passe o caminho aqui se a função get_fighters_list
        # já tiver o caminho interno do container (os.path.join...)
        return get_fighters_list()

    @task
    def scrape_fighter_task(fighter_entry):
        # Passa o item do mapeamento para a função do seu include
        return scrape_single_fighter(fighter_entry)

    @task
    def save_to_bronze_task(all_fighter_details):
        import os

        import pandas as pd

        clean_data = [d for d in all_fighter_details if d is not None]

        # Caminho relativo direto à raiz do projeto Astro
        file_path = "include/data/bronze/fighters.csv"

        # Garante a criação da pasta
        os.makedirs("include/data/bronze", exist_ok=True)

        df = pd.DataFrame(clean_data)
        df.to_csv(file_path, index=False)
        print(f"Confirmando escrita no arquivo: {os.path.abspath(file_path)}")

    # Execução do Fluxo
    fighters = list_fighters_task()
    details = scrape_fighter_task.expand(fighter_entry=fighters)
    save_to_bronze_task(details)


mma_modern_pipeline()
