from utils import db_connect
engine = db_connect()

# your code here
#EXPORTAR DATOS 
tit= "https://raw.githubusercontent.com/4GeeksAcademy/k-nearest-neighbors-project-tutorial/main/tmdb_5000_movies.csv"
cred= "https://raw.githubusercontent.com/4GeeksAcademy/k-nearest-neighbors-project-tutorial/main/tmdb_5000_credits.csv"
titulos= pd.read_csv(tit)
creditos= pd.read_csv(cred)
titulos.head()

creditos.head()

#CREAR BASES DE DATOS 
conn= sqlite3.connect("/workspace/Knearest/data/database.db")
titulos.to_sql("titulos_db", conn, if_exist= "replace", index=False)
creditos.to_sql("creditos_db", conn, f_exist= "replace" ,index= False)

#Aquí he pensado que para evitar uni las tablas a posteriori se puede poner en if_exist= app

#UNIR TABLAS EN SQL Y SACAR DF 
query= """ SELECT * 
FROM titulos_db
INNER JOIN creditos_db
ON titulos_db.title= creditos_db.title"""

peliculas_df= pd.read_sql_query(query, conn)
conn.close()

peliculas_df.head()

peliculas_df_final= peliculas_df[["movie_id", "title", "overview", "genres", "keywords", "cast", "crew"]]
peliculas_df_final.head()

peliculas_df_final.drop_duplicates()

#LIMPIEZA DE DATOS Y TRATAMIENTO JSON


def load_json_safe(json_str, default_value = None):
    try:
        return json.loads(json_str)
    except (TypeError, json.JSONDecodeError):
        return default_value
    
peliculas_df_final["genres"] = peliculas_df_final["genres"].apply(lambda x: [item["name"] for item in json.loads(x)] if pd.notna(x) else None)
peliculas_df_final["keywords"] = peliculas_df_final["keywords"].apply(
    lambda x: [item["name"] for item in json.loads(x)] if isinstance(x, str) and pd.notna(x) else None
)

peliculas_df_final["cast"] = peliculas_df_final["cast"].apply(
    lambda x: [item["name"] for item in json.loads(x)] [:3] if isinstance(x, str) and pd.notna(x) else None
)

peliculas_df_final["crew"] = peliculas_df_final["crew"].apply(
    lambda x: " ".join([crew_member['name'] for crew_member in load_json_safe(x) if crew_member['job'] == 'Director']) if x is not None else ""
)

peliculas_df_final["overview"] = peliculas_df_final["overview"].apply(lambda x: [x])

peliculas_df_final.head()