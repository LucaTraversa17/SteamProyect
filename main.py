from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
import pandas as pd

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
async def get_estadisticas(request: DeveloperRequest):
    try:
        estadisticas = calcular_estadisticas_por_desarrollador(request.developer)
        return estadisticas.to_dict(orient="index")
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
