from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
import pandas as pd
import uvicorn 

app = FastAPI()


df_juegos = pd.read_parquet('df_consulta_free.parquet')  

def calcular_estadisticas_por_desarrollador(desarrollador):
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

class DeveloperRequest(BaseModel):
    developer: str

@app.post("/estadisticas/")
def get_estadisticas_por_desarrollador(request: DeveloperRequest):
    estadisticas = calcular_estadisticas_por_desarrollador(request.developer)
    if estadisticas is None:
        raise HTTPException(status_code=404, detail="Developer not found")
    return estadisticas.to_dict()

