from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
import pandas as pd
import uvicorn 
from fastapi.responses import RedirectResponse
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.metrics.pairwise import cosine_similarity

app = FastAPI()

# Definir las funciones de estadísticas
def developer_stats(desarrollador):
    df = pd.read_parquet('df_consulta_free.parquet')
    df = df[df['developer']== desarrollador]
    counts = df.groupby('release_date').agg(
    total_values=pd.NamedAgg(column='price', aggfunc='size'),
    free_values=pd.NamedAgg(column='price', aggfunc=lambda x: (x == 0).sum())
    )
    counts['percentage_free'] = round((counts['free_values'] / counts['total_values']) * 100,2)
    counts = counts.to_dict()
    return counts


def user_statistics(user_id):
    df = pd.read_parquet('df_consulta_gasto_usuario.parquet')
    # Filtrar el DataFrame para el usuario dado
    df = df[df['user_id'] == user_id]
    # Calcular las métricas
    total_spent = df['price'].sum()
    recommendation_rate = df['recommend'].mean() * 100
    item_count = df['item_id'].count()
    resultado = {
        'Usuario': user_id,
        'Monto Gastado': f'USD:${round(total_spent,2)}',
        'Ratio de recomendacion': f"{round(recommendation_rate,2)}%",
        'Items comprados': int(item_count)
    }
    return resultado


def estadistica_genero(genero):
    df = pd.read_parquet('df_consulta_genero.parquet')
    df = df[df['genres'].isin([genero])]
    usuario = df.groupby('user_id')['playtime_forever'].sum().idxmax()
    df= df[df['user_id']==usuario]
    df = df.groupby(['user_id', 'release_date'])['playtime_forever'].sum().reset_index()
    # Crear un diccionario para almacenar los resultados
    resultado = {}
    # Iterar sobre los grupos y construir el diccionario
    for index, row in df.iterrows():
        año = row['release_date']
        tiempo = row['playtime_forever']

        resultado[año] = tiempo
    resultado = {f"El usuario que más jugó al género {genero} fue {usuario}": [resultado]}

    return resultado

def best_developer_year(año: int) -> dict:
    df = pd.read_parquet('df_consulta_positivo_desarrollador.parquet')
    # Filtrar por el año especificado
    df = df[df['year'] == año]
    # Agrupar por año y desarrollador, y contar los juegos
    df = df.groupby(['year', 'developer']).size().reset_index(name='count').sort_values(by='count', ascending=False)
    # Seleccionar los tres mejores desarrolladores
    df = df.iloc[:3, :2].reset_index(drop=True)
    # Crear el diccionario de resultados
    result = {f"Puesto {idx + 1}": row['developer'] for idx, row in df.iterrows()}
    return result

def developer_reviews_analysis(desarrolladora: str):
    df = pd.read_parquet('df_consulta_sentimientos_desarrollador.parquet')
    df = df[df['developer'] == desarrolladora]
    count_2 = (df['sentiment'] == 2).sum()
    count_0 = (df['sentiment'] == 0).sum()
    resultado = {
        f'Desarrollador: {desarrolladora}': [f'Positivas: {count_2}', f'Negativas: {count_0}']
    }
    return resultado

def get_recommendations(item_id):
    df = pd.read_parquet('df_consulta_final.parquet')
    tfidf = TfidfVectorizer(stop_words='english')
    tfidf_matrix = tfidf.fit_transform(df['combined_features'])
    cosine_sim = cosine_similarity(tfidf_matrix, tfidf_matrix)
    idx = df.index[df['item_id'] == item_id].tolist()[0]
    sim_scores = list(enumerate(cosine_sim[idx]))
    sim_scores = sorted(sim_scores, key=lambda x: x[1], reverse=True)
    sim_scores = sim_scores[1:6]  # Get top 5 recommendations
    game_indices = [i[0] for i in sim_scores]
    resultado = df['app_name'].iloc[game_indices].reset_index(drop=True)
    resultado.index += 1  # Cambia el índice para que empiece en 1
    resultado = resultado.to_dict()
    return resultado

# Definir las clases de solicitud
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

@app.get("/estadisticas/developer/")
async def developer(developer: str):
    estadisticas = developer_stats(developer)
    if estadisticas is None:
        raise HTTPException(status_code=404, detail="Developer not found")
    return estadisticas

@app.get("/estadisticas/user/")
async def user(user_id: str):
    estadisticas = user_statistics(user_id)
    if estadisticas is None:
        raise HTTPException(status_code=404, detail="User not found")
    return estadisticas

# Definir los endpoints utilizando GET
@app.get("/estadisticas/genero/")
async def genero(genre: str):
    estadisticas = estadistica_genero(genre)
    if estadisticas is None:
        raise HTTPException(status_code=404, detail="Genre not found")
    return estadisticas

@app.get("/estadisticas/best_developer_year/")
async def best_developer(year: int):
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

@app.get("/estadisticas/recomendacion/")
async def recomendacion(item: int):
    recomendacion = get_recommendations(item)
    if recomendacion is None:
        raise HTTPException(status_code=404, detail="Item not found")
    return recomendacion

