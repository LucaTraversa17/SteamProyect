import pandas as pd
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel

app = FastAPI()

def user_statistics(user_id):
    try:
        # Verificar si el archivo existe y es accesible
        df = pd.read_parquet('df_consulta_gasto_usuario.parquet')
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error al leer el archivo parquet: {e}")

    try:
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
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error al calcular las estadísticas: {e}")

class UserRequest(BaseModel):
    user_id: str

@app.get("/estadisticas/user/")
async def user(user_id: str):
    estadisticas = user_statistics(user_id)
    return estadisticas
