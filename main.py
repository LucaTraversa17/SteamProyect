from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
import pandas as pd
import uvicorn 
from fastapi.responses import RedirectResponse

app = FastAPI()

# Definir las funciones de estadísticas
def developer_stats(desarrollador):
    df_juegos = pd.read_parquet('df_consulta_free.parquet')
    #volver a hacerla
    return df_juegos

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

# Definir las clases de solicitud
class DeveloperRequest(BaseModel):
    developer: str

class UserRequest(BaseModel):
    user_id: str

class GenreRequest(BaseModel):
    genre: str

class YearRequest(BaseModel):
    year: int

class DeveloperNameRequest(BaseModel):
    developer: str

# Definir los endpoints
@app.post("/estadisticas/developer/")
async def developer(request: DeveloperRequest):
    estadisticas = developer_stats(request.developer)
    if estadisticas is None:
        raise HTTPException(status_code=404, detail="Developer not found")
    return estadisticas.to_dict()

@app.post("/estadisticas/user/")
async def user(request: UserRequest):
    estadisticas = user_statistics(request.user_id)
    if estadisticas is None:
        raise HTTPException(status_code=404, detail="User not found")
    return estadisticas

@app.post("/estadisticas/genero/")
async def genero(request: GenreRequest):
    estadisticas = estadistica_genero(request.genre)
    if estadisticas is None:
        raise HTTPException(status_code=404, detail="Genre not found")
    return estadisticas

@app.post("/estadisticas/best_developer_year/")
async def best_developer(request: YearRequest):
    estadisticas = best_developer_year(request.year)
    if estadisticas is None:
        raise HTTPException(status_code=404, detail="Year not found")
    return estadisticas

@app.post("/estadisticas/developer_reviews/")
async def developer_reviews(request: DeveloperNameRequest):
    estadisticas = developer_reviews_analysis(request.developer)
    if estadisticas is None:
        raise HTTPException(status_code=404, detail="Developer not found")
    return estadisticas

