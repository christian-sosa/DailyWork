import pandas as pd

mydict = [
    {"a": 1, "b": 2, "c": 3, "d": 4},
    {"a": 10000, "b": 200, "c": 300, "d": 400},
    {"a": 1000, "b": 2000, "c": 3000, "d": 4000},
]
df = pd.DataFrame(mydict)

# df.shape: Esta propiedad devuelve una tupla con el número de filas y columnas de un DataFrame.
print("SHAPE")
print(df.shape)
# df.columns: Esta propiedad devuelve una lista con los nombres de las columnas de un DataFrame.
print("columns")
print(df.columns)
# df.info(): Esta función se utiliza para ver información general sobre un DataFrame, como el número de filas, el número de columnas
# , el tipo de datos de cada columna y la cantidad de valores no nulos.
print("info")
print(df.info())
# df.describe(): Esta función se utiliza para ver estadísticas resumen de las columnas numéricas de un DataFrame.
print("describe")
print(df.describe())
# df.sort_values(): Esta función se utiliza para ordenar un DataFrame por una o varias columnas.

# df.sort_values(by=['columna1','columna2'], ascending=[True,False])
# df.groupby(): Esta función se utiliza para agrupar un DataFrame por una o varias columnas y aplicar una función de agregación a cada grupo.
print(df.groupby("a").mean())
