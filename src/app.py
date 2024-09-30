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
    
peliculas_df_final["genres"] = peliculas_df_final["genres"].apply(lambda x: [item["name"] for item in json.loads(x)])
peliculas_df_final["keywords"] = peliculas_df_final["keywords"].apply(lambda x: [item["name"] for item in json.loads(x)])
peliculas_df_final["cast"] = peliculas_df_final["cast"].apply(lambda x: [item["name"] for item in json.loads(x)])

peliculas_df_final["crew"] = peliculas_df_final["crew"].apply(lambda x: " ".join([crew_member['name'] for crew_member in load_json_safe(x) if crew_member['job'] == 'Director']))
peliculas_df_final["overview"] = peliculas_df_final["overview"].apply(lambda x: [x])

peliculas_df_final.head()

#TRANSFORMACIÓN OVERVIEW, GENRES, KEYOWORDS, 
peliculas_df_final["overview"] = peliculas_df_final["overview"].apply(lambda x: [str(x)])
peliculas_df_final["genres"] = peliculas_df_final["genres"].apply(lambda x: [str(genre) for genre in x])
peliculas_df_final["keywords"] = peliculas_df_final["keywords"].apply(lambda x: [str(keyword) for keyword in x])
peliculas_df_final["cast"] = peliculas_df_final["cast"].apply(lambda x: [str(actor) for actor in x])
peliculas_df_final["crew"] = peliculas_df_final["crew"].apply(lambda x: [str(crew_member) for crew_member in x])

peliculas_df_final["tags"] = peliculas_df_final["overview"] + peliculas_df_final["genres"] + peliculas_df_final["keywords"] + peliculas_df_final["cast"] + peliculas_df_final["crew"]
peliculas_df_final["tags"] = peliculas_df_final["tags"].apply(lambda x: ",".join(x).replace(",", " "))

peliculas_df_final.drop(columns = ["genres", "keywords", "cast", "crew", "overview"], inplace = True)

peliculas_df_final.iloc[0].tags
peliculas_df_final["tags"]

#KNN 
vector = TfidfVectorizer()
matriz = vector.fit_transform(peliculas_df_final["tags"])

modelo = NearestNeighbors(n_neighbors = 4, algorithm = "brute", metric = "minkowski")
modelo.fit(matriz)

def get_movie_recommendations(movie_title):
    movie_index = peliculas_df_final[peliculas_df_final["title"] == movie_title].index[0]
    distances, indices = modelo.kneighbors(matriz[movie_index])
    similar_movies = [(peliculas_df_final["title"][i], distances[0][j]) for j, i in enumerate(indices[0])]
    return similar_movies[1:]

input_movie = "Avatar"
recommendations = get_movie_recommendations(input_movie)
print("Peliculas recomendadas por que te gusta '{}'".format(input_movie))
for movie, distance in recommendations:
    print("- Film: {}".format(movie))