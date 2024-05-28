from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
import pandas as pd
import uvicorn 
from fastapi.responses import RedirectResponse
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.metrics.pairwise import cosine_similarity

#Instanciamos el objeto de FastAPI, después lo usaremos con uvicorn
app = FastAPI()

# Definir las funciones de estadísticas
# Cada funcion tiene su Dataset particular para que sea mas eficiente. Luego dentro del Dataset, nos quedaremos con un subset producto del input que se introduzca.

# Funcion N°1 estadisticas del desarrollador.
def developer_statistics(desarrollador):
    #Leemos el Dataset previamente depurado
    df = pd.read_parquet('Datasets\df_consulta_free.parquet')
    #Nos quedamos unicamente con la información del desarrollador introducido. 
    df = df[df['developer']== desarrollador]
    if df.empty:
        return None
    #Calculamos los valores que nos interesan agrupados por año: titulos totales y titulos gratuitos
    counts = df.groupby('release_date').agg(
    total_values=pd.NamedAgg(column='price', aggfunc='size'),
    free_values=pd.NamedAgg(column='price', aggfunc=lambda x: (x == 0).sum())
    )
    #Si no se encuentra el developer, va a retornar nulo. 

    #Sacamos el % de los titulos gratuitos sobre el total. 
    counts['percentage_free'] = round((counts['free_values'] / counts['total_values']) * 100,2)
    #Lo pasamos a dict para que la API retorne un output valido. 
    counts = counts.to_dict()
    return counts

#Funcion N°2 estadisticas del usuario. 
def user_statistics(user_id):
    df = pd.read_parquet('Datasets\df_consulta_gasto_usuario.parquet')
    # Filtrar el DataFrame para el usuario dado
    df = df[df['user_id'] == user_id]
    if df.empty:
        return None
    # Calcular las métricas del total gastado
    total_spent = df['price'].sum()
    #Porcentaje de recomendación.
    recommendation_rate = df['recommend'].mean() * 100
    #Total de items.
    item_count = df['item_id'].count()
    #Convertimos todo en un diccionario para que sea output valido. 
    resultado = {
        'Usuario': user_id,
        'Monto Gastado': f'USD:${round(total_spent,2)}',
        'Ratio de recomendacion': f"{round(recommendation_rate,2)}%",
        'Items comprados': int(item_count)
    }
    return resultado

# Función N°3 estadísticas por género.
def genre_statistics(genero):
    df = pd.read_parquet('Datasets\df_consulta_genero.parquet')
    #Dataset unicamente del genero introducido. 
    df = df[df['genres'].isin([genero])]
    if df.empty:
        return None
    #Nos quedamos eon el usuario que más haya jugado a ese género, sumando las horas de playtime_forever.
    usuario = df.groupby('user_id')['playtime_forever'].sum().idxmax()
    #Creamos un subset del usuario en cuestion.
    df= df[df['user_id']==usuario]
    #Agrupamos por año la cantidad de tiempo jugado.
    df = df.groupby('release_date')['playtime_forever'].sum().reset_index()
    df['release_date'] = df['release_date'].astype(str)
    # Iterar sobre las variables y construir el diccionario
    resultado = {}
    for index, row in df.iterrows():
        año = row['release_date']
        tiempo = row['playtime_forever']
        resultado[año] = tiempo
    resultado = {f"El usuario que más jugó al género {genero} fue {usuario}": [resultado]}

    return resultado

# Función N°4 estadísticas por año.
def best_developer_year(año):
    df = pd.read_parquet('Datasets\df_consulta_positivo_desarrollador.parquet')
    # Filtrar por el año especificado
    df = df[df['year'] == año]
    if df.empty:
        return None
    # Agrupar por año y desarrollador, y contar los juegos
    df = df.groupby(['year', 'developer']).size().reset_index(name='count').sort_values(by='count', ascending=False)
    # Seleccionar los tres mejores desarrolladores
    df = df.iloc[:3, :2].reset_index(drop=True)
    # Crear el diccionario de resultados
    result = {f"Puesto {idx + 1}": row['developer'] for idx, row in df.iterrows()}
    return result

# Función N°5 reviews positivas y negativas por desarrollador.
def developer_reviews_analysis(desarrolladora):
    #Creamos el dataset con la información del desarrollador
    df = pd.read_parquet('Datasets\df_consulta_sentimientos_desarrollador.parquet')
    df = df[df['developer'] == desarrolladora]
    if df.empty:
        return None
    #Contamos unicamente los puntaje 2 (positivo) y los 0 (negativo)
    count_2 = (df['sentiment'] == 2).sum()
    count_0 = (df['sentiment'] == 0).sum()
    resultado = {
        f'Desarrollador: {desarrolladora}': [f'Positivas: {count_2}', f'Negativas: {count_0}']
    }
    return resultado

def get_recommendations(item_id):
    #Abrimos el Dataset. La columna mas importante es combined features que incluye todos los tags, specs y generos de cada juego. Sera la clave para el sistema de recomendación. 
    df = pd.read_parquet('Datasets\df_consulta_final.parquet')
    #Creamos el sistema con la ayuda de la biblioteca sklearn. 
    tfidf = TfidfVectorizer(stop_words='english')
    tfidf_matrix = tfidf.fit_transform(df['combined_features'])
    cosine_sim = cosine_similarity(tfidf_matrix, tfidf_matrix)
    idx = df.index[df['item_id'] == item_id].tolist()[0]
    sim_scores = list(enumerate(cosine_sim[idx]))
    sim_scores = sorted(sim_scores, key=lambda x: x[1], reverse=True)
    #Nos quedamos con el top del score del sistema de recomendacion. 
    sim_scores = sim_scores[1:6]
    #Extraemos el indice
    game_indices = [i[0] for i in sim_scores]
    #Pasamos el indice al DF para obtener la informacion del juego
    resultado = df['app_name'].iloc[game_indices].reset_index(drop=True)
    #Agregamos 1 al indice para que quede del 1 al 5 en el diccionario. 
    resultado.index += 1 
    resultado = resultado.to_dict()
    return resultado

# Definir las clases de solicitud para la API.
class DeveloperRequest(BaseModel):
    developer: str

class UserRequest(BaseModel):
    user_id: str

class GenreRequest(BaseModel):
    genre: str

class YearRequest(BaseModel):
    year: int

class Item_id(BaseModel):
    item: int

#Creamos, con la ayuda de la biblioteca FASTAPI, los endpoint para cada función. 

@app.get("/estadisticas/developer_statistics/")
async def developers_stats(developer: str):
    estadisticas = developer_statistics(developer)
    if estadisticas is None:
        raise HTTPException(status_code=404, detail="Developer not found")
    return estadisticas

@app.get("/estadisticas/user_statistics/")
async def users_statistics(user_id: str):
    estadisticas = user_statistics(user_id)
    if estadisticas is None:
        raise HTTPException(status_code=404, detail="User not found")
    return estadisticas

@app.get("/estadisticas/genre_statistics/")
async def genres_statistics(genre: str):
    estadisticas = genre_statistics(genre)
    if estadisticas is None:
        raise HTTPException(status_code=404, detail="Genre not found")
    return estadisticas

@app.get("/estadisticas/best_developer_year/")
async def best_developers_year(year: int):
    estadisticas = best_developer_year(year)
    if estadisticas is None:
        raise HTTPException(status_code=404, detail="Year not found")
    return estadisticas

@app.get("/estadisticas/developer_reviews/")
async def developer_reviews(developer: str):
    estadisticas = developer_reviews_analysis(developer)
    if estadisticas is None:
        raise HTTPException(status_code=404, detail="Developer not found")
    return estadisticas

@app.get("/estadisticas/get_recomendation/")
async def get_recomendation(item: int):
    recomendacion = get_recommendations(item)
    if recomendacion is None:
        raise HTTPException(status_code=404, detail="Item not found")
    return recomendacion

