from airflow.providers.postgres.hooks.postgres import PostgresHook
import pandas as pd
import logging


class PostgresLoader:
    def __init__(self, conn_id="postgres_local"):
        self.conn_id = conn_id
        self.hook = PostgresHook(postgres_conn_id=self.conn_id)

    def load_to_db(self, csv_path, schema, table_name):
        """Carrega um CSV para um schema e tabela específicos."""
        try:
            df = pd.read_csv(csv_path)
            engine = self.hook.get_sqlalchemy_engine()

            df.to_sql(name=table_name, con=engine, schema=schema, if_exists="replace", index=False)
            logging.info(f"Sucesso: {len(df)} linhas em {schema}.{table_name}")
        except Exception as e:
            logging.error(f"Falha na carga: {e}")
            raise e
