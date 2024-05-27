# SteamProyect
## Contexto
Hemos recibido 3 Datasets de la empresa Steam que nos encargó llevar a cabo una serie de análisis para evaluar distintas métricas de sus productos. Las fases del proceso que debemos llevar a cabo son las siguientes:

En primer lugar deberemos realizar un ETL de los archivos. Como hemos mencionado tenemos tres archivos que nos brindarán distinta información. Sin embargo, estos archivos no están guardados de manera correcta y por lo tanto deberemos llevaer a cabo una adecuada descompresión y limpieza de los mismos. 

En segundo lugar, una vez que tengamos los archivos correctamente legibles, generaremos un Dataset con la información que va a requerir cada función. Este proceso también implicará un proceso de EDA ya que deberemos decidir qué hacemos con los campos nulos y con los campos duplicados por ejemplo. La creación de Datasets para cada función nos proporcionará mayor eficiencia computacional ya que eliminaríamos la información que no es relevante para cada función. 

En tercer lugar, nos han solicitado generar un sistema de recomendación que aplique la similitud del coseno. Debremos crear una función que nos recomiende distintos productos según el producto que le introduzcamos como parámetro. Para es última función, la más importante del proyecto, también generaremos un Dataset específico con su correspondiente EDA y ETL.

Finalmente, una vez que tengamos la funciones, generaremos una API con FastApi donde podamos consultar las mismas. Además se nos ha solicitado que deployemos en Render una versión online de nuestra API que pueda ser consumida desde cualquier lugar. Debido a las limitaciones de memoria y procesamiento de Render, tendremos que limitar el Dataset del sistema de recomendación para que pueda funcionar (más sobre este tema en el apartado de funciones). 

### Organización del repositorio 

En el repositorio se podrán observar los siguientes archivos:
* Carpeta con los DataSets en formato parquet para su optimización. Son seis Datasets, uno para cada función.
* Archivo "Exploración_Juegos.ipynb" en el cual se lleva a cabo el la descompresión de los Datasets originales, su análisis y la creación de los datasets finales.
* Archivo "EDA.ipynb" donde se lleva a cabo un análisis exploratorio para entender mejor los Datasets para las funciones. 
* Archivo "main.py" que contiene la creación de la API con las funciones. Este archivo será consumido por Render para su deploy.
* Archivo "requirements.txt" que Render utilizará para descargar las bibliotecas correspondientes para que la API funcione.
* [Link al deploy de Render](https://streamproyect.onrender.com/docs) 

## Funciones 
La API consta de 6 funciones:
| Nombre | Input | Output |
|-----------|-----------|-----------|
| Developer_stats | String con nombre de desarrollador | Elementos por año del desarrolador, totales, gratuitos y % de gratuitos |
| User_statistics | String con nombre de usuario| Cuanto gastó en Steam, en cuántos elementos, y cuántos recomendó del total|
| genre_statistics | String con un género de videojuego | Cuál fue el usuario que más jugó ese generó y cuánto jugó por año|
| best_developer_year | Integer con un año| Los tres desarrolladores con mayor cantidad de recomendaciones para ese año|
| developer_reviews_analysis| String con nombre de desarrollador| Cuántos reviews positivos y negativos tuvo ese desarrollador*|
| get_recommendations |Integer con un ID de juego | Recomendación de cinco juegos similares al ingresado aplicando similitud del coseno**|

### Notas sobre las funciones
*Para esta función se llevo a cabo un procesamiento de lenguaje natural sobre las reviews, asignandole un valor Negativo, Neutro o Positivo. Las valoraciones neutras no se contabilizaron para la función, únicamente las positivas o negativas. 

**Debido a la gran cantidad de elementos del Dataset y la poca capacidad de procesamiento de Render, el sistema de recomendación se enfoca únicamente en los 4500 juegos más jugados de la plataforma. Entendemos que es un número significativo y relevante, eliminando los juegos que no son elegidos por los usuarios. 
