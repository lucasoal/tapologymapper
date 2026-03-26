from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

from include.src.ingestion.fighter_scraper import run_ingestion
from include.src.processing.data_cleasing import run_cleasing

with DAG(
    dag_id="dagtestarch",
    start_date=datetime(2026, 3, 26),
    schedule=None,
    catchup=False,
    tags=["teste", "arquiteturaaaa"],
) as dag:

    # task 1 - ingestao
    task_ingestao = PythonOperator(
        task_id="ingestaooo",
        python_callable=run_ingestion,
    )

    # task 2 - cleasing
    task_cleasing = PythonOperator(
        task_id="cleasiningisna",
        python_callable=run_cleasing,
    )

    task_ingestao >> task_cleasing

# ============================================================
# ========================= 2 versao =========================
# ============================================================

# A diferença principal é que o exemplo do Astro usa a TaskFlow API
# (introduzida no Airflow 2.0), enquanto a sua DAG estava usando o 
# modelo Tradicional (PythonOperator).
# No modelo TaskFlow (usado no exemplo), você usa decoradores como 
# @dag e @task. Nesse formato, o Airflow transforma o nome da função
# automaticamente no task_id. No modelo Tradicional (o que você 
# escreveu), o task_id precisa ser declarado explicitamente 
# dentro do operador.


# from airflow.decorators import dag, task
# from datetime import datetime

# # Seus imports do include continuam iguais
# from include.src.ingestion.fighter_scraper import run_ingestion
# from include.src.processing.data_cleansing import run_cleansing

# @dag(
#     dag_id="dagtestarch_taskflow",
#     start_date=datetime(2026, 3, 26),
#     schedule=None,
#     catchup=False,
#     tags=["teste", "arquitetura_moderna"],
# )
# def medallion_pipeline():

#     @task()
#     def task_ingestao():
#         # Aqui você chama a sua função do include
#         return run_ingestion()

#     @task()
#     def task_cleansing():
#         # Aqui você chama a sua função do include
#         return run_cleansing()

#     # Define a ordem de execução
#     task_ingestao() >> task_cleansing()


# # Invoca a função da DAG para o Airflow registrá-la
# medallion_pipeline()
