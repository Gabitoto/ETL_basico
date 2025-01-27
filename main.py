# Proceso de ETL desde un archivo a una BBDD

import pandas as pd
import datetime
import sqlite3

def log_progress(message):
    with open('code_log.txt', 'a') as log_file:
        log_file.write(f"{datetime.datetime.now()} : {message}\n")


def extract(url):
    try:
        
        # Convierte la tabla CSV en un DataFrame
        data = pd.read_csv(url, sep=";")
        
        # Limpia y devuelve el DataFrame
        data.columns = data.columns.str.strip()  # Elimina espacios en los nombres de las columnas
        return data

    except Exception as e:
        print(f"Error en la función extract: {e}")
        return None

def transform(df):

    try:
        # Dejamos las columnas que seleccionamos y eliminamos valores faltantes
        df = df.dropna() # Eliminamos los valores faltantes
        new_df = df[['id', 'title', "revenue", "budget", "genres"]] # Solo dejamos las columnas que nos interesan
        return new_df

    except Exception as e:
        print(f"Error en la función transform: {e}")
        return None


def load_to_csv(df, output_path):
    try:
        df.to_csv(output_path, index=False)
        log_progress("Archivo CSV guardado correctamente.")
    except Exception as e:
        log_progress(f"Error guardando CSV: {e}")

def load_to_db(df, db_name, table_name):
    try:
        conn = sqlite3.connect(db_name)
        df.to_sql(table_name, conn, if_exists='replace', index=False)
        conn.close()
        log_progress("Datos cargados en la base de datos correctamente.")
    except Exception as e:
        log_progress(f"Error cargando en la base de datos: {e}")

def run_queries(db_name):
    try:
        conn = sqlite3.connect(db_name)
        queries = [
            "SELECT * FROM MoviesIBDM;",
            "SELECT AVG(revenue) AS avg_revenue FROM MoviesIBDM;",
            "SELECT title FROM MoviesIBDM LIMIT 5;"
        ]

        for query in queries:
            print(f"Query: {query}")
            print(pd.read_sql_query(query, conn))
        conn.close()
    except Exception as e:
        log_progress(f"Error ejecutando consultas: {e}")

if __name__ == "__main__":
    log_progress("Preliminares completos. Iniciando proceso ETL")
    url = r"C:\projects\proyecto_ETL\data\Imdb Movie Dataset.csv"

    output_csv = "./New_Data_Movies.csv"
    db_name = "Movies.db"
    table_name = "MoviesIBDM"

df = extract(url)
if df is not None:
    df = transform(df)
    if df is not None:
        load_to_csv(df, output_csv)
        load_to_db(df, db_name, table_name)
        run_queries(db_name)