from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
import pandas as pd
import uvicorn 
from fastapi.responses import RedirectResponse

app = FastAPI()


# Definir las funciones de estadísticas
def developer_stats(desarrollador):
    df_juegos = pd.read_parquet('df_consulta_free.parquet')
    df_desarrollador = df_juegos[df_juegos['developer'] == desarrollador]
    total_items_por_anio = df_desarrollador.groupby('release_date')['app_name'].count()
    items_gratuitos_por_anio = df_desarrollador[df_desarrollador['price'] == 0].groupby('release_date')['app_name'].count()
    estadisticas = pd.DataFrame({
        'Cantidad Gratuitos': items_gratuitos_por_anio,
        'Cantidad Total': total_items_por_anio,
        'Porcentaje Gratuito': (items_gratuitos_por_anio / total_items_por_anio) * 100
    })
    estadisticas = estadisticas.fillna(0)
    estadisticas = estadisticas.rename_axis(index={'release_date': 'Año'})
    return estadisticas

def user_statistics(user_id):
    df = pd.read_parquet('df_consulta_gasto_usuario.parquet')
    # Filtrar el DataFrame para el usuario dado
    df = df[df['user_id'] == user_id]
    if df.empty:
        return {
            'user_id': user_id,
            'total_spent': 0.0,
            'recommendation_rate': 0.0,
            'item_count': 0
        }
    # Calcular las métricas
    total_spent = df['price'].sum()
    recommendation_rate = df['recommend'].mean() * 100
    item_count = df['item_id'].count()
    return {
        'Usuario': user_id,
        'Monto Gastado': f'USD:${round(total_spent,2)}',
        'Ratio de recomendacion': f"{round(recommendation_rate,2)}%",
        'Items comprados': int(item_count)
    }


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

    return {f"El usuario que más jugó al género {genero} fue {usuario}": [resultado]}

def best_developer_year(año: int):
    df = pd.read_parquet('df_consulta_positivo_desarrollador.parquet')
    df = df[df['year'] == año]
    df = df.groupby(['year', 'developer']).size().reset_index(name='count').sort_values(by='count', ascending=False)
    df = df.iloc[:3, :2].reset_index(drop=True)
    result = []
    for idx, row in df.iterrows():
        result.append({f"Puesto {idx + 1}": row['developer']})
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
def developer(request: DeveloperRequest):
    estadisticas = developer_stats(request.developer)
    if estadisticas is None:
        raise HTTPException(status_code=404, detail="Developer not found")
    return estadisticas.to_dict()

@app.post("/estadisticas/user/")
def user(request: UserRequest):
    estadisticas = user_statistics(request.user_id)
    if estadisticas is None:
        raise HTTPException(status_code=404, detail="User not found")
    return estadisticas

@app.post("/estadisticas/genero/")
def genero(request: GenreRequest):
    estadisticas = estadistica_genero(request.genre)
    if estadisticas is None:
        raise HTTPException(status_code=404, detail="Genre not found")
    return estadisticas

@app.post("/estadisticas/best_developer_year/")
def best_developer(request: YearRequest):
    estadisticas = best_developer_year(request.year)
    if estadisticas is None:
        raise HTTPException(status_code=404, detail="Year not found")
    return estadisticas

@app.post("/estadisticas/developer_reviews/")
def developer_reviews(request: DeveloperNameRequest):
    estadisticas = developer_reviews_analysis(request.developer)
    if estadisticas is None:
        raise HTTPException(status_code=404, detail="Developer not found")
    return estadisticas

# Redirigir automáticamente a /docs
@app.get("/", include_in_schema=False)
def read_root():
    return RedirectResponse(url="/docs")

if __name__ == "__main__":
    uvicorn.run(app, host="127.0.0.1", port=8000)
