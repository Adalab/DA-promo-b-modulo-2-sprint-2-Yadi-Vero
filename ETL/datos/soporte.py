# de nuevo, en este archivo deberemos poner todas las librerias que necesitamos para que se ejecute el código correctamente. 

import requests
import pandas as pd
import numpy as np
from datetime import datetime, timedelta

class Extraccion: 
    
    def __init__(self, lat, lon, ciudad):
        self.lat = lat
        self.lon = lon
        self.ciudad = ciudad

        
    def llamada_API(self, producto):
        
        self.producto = producto
    
        # hacemos la llamada  a la API
        url = f'http://www.7timer.info/bin/api.pl?lon=-{self.lon}&lat={self.lat}&product={producto}&output=json'

        response = requests.get(url=url)
        codigo_estado = response.status_code
        razon_estado = response.reason
        if codigo_estado == 200:
            print('La peticion se ha realizado correctamente, se ha devuelto el código de estado:',codigo_estado,' y como razón del código de estado: ',razon_estado)
        elif codigo_estado == 402:
            print('No se ha podido autorizar usario, se ha devuelto el código de estado:', codigo_estado,' y como razón del código de estado: ',razon_estado)
        elif codigo_estado == 404:
            print('Algo ha salido mal, el recurso no se ha encontrado,se ha devuelto el código de estado:', codigo_estado,' y como razón del código de estado: ',razon_estado)
        else:
            print('Algo inesperado ha ocurrido, se ha devuelto el código de estado:', codigo_estado,' y como razón del código de estado: ',razon_estado)

        # convertimos los resultados en un dataframe: 
        df = pd.DataFrame.from_dict(pd.json_normalize(response.json()['dataseries']))
        return df
    
    def limpiar_civil(self, df): 

        # lo primero que tenemos que hacer es crear la columna de fecha en el dataframe nuevo 
        # En este caso será la fecha del día de hoy. 
        hoy = datetime.now()
        hoy = datetime.strftime(hoy, '%Y-%m-%d')

        # creamos la nueva columna
        df["fecha"] = hoy

        df["latitud"] = self.lat
        df["longitud"] = self.lon
        df["ciudad"] = self.ciudad
        return df
    
    
    
    def limpiar_astro(self, df):

        #seleccionamos solo las columnas que nos interesan
        df = df[["seeing", "transparency", "timepoint"]]

        # creamos la columna de fecha: 
        hoy = datetime.now()
        hoy = datetime.strftime(hoy, '%Y-%m-%d')
        df["fecha"] = hoy


        # insertamos las columnas de la localidad
    
        df["ciudad"] = self.ciudad

        return df
    
    
    def juntar_dfs(self, df_completo, df_civil, df_visibilidad): 

        df_hoy = pd.merge(df_civil , df_visibilidad , on=['fecha', "timepoint"], how = "inner")
        print("El df de hoy es ", df_hoy.columns)
        print("-----------------------------------------")
        print("El df de completo es ", df_completo.columns)
        # lo primero que hacemos es concatenar los dataframes con la información general. df_completo y df_civil
        df_completo = pd.concat([df_completo, df_hoy], axis = 0)

        # ahora es el turno de unir el dataframe con la información de la visibilidad con el completo
        # en este caso el how lo ponemos como left ya que queremos que se quede con toda la info que en la primera tabla que le pasamos que es la que tiene toda la información
        #df_completo = pd.merge(df_completo , df_visibilidad , on=['fecha', "timepoint"], how = "inner")

        # guardamos los datos
        df_completo.to_pickle('datos/datos_actualizados.pkl')
        df_completo.to_csv('datos/datos_actualizados.csv')

        return df_completo
    
    

    def chequear_datos(self, df): 
    
        print("Las columnas son:", "\n")
        print(list(df.columns))
        print("-----------------------------------------")

        print("Los tipos de datos que tenemos son:", "\n")
        print(df.dtypes)
        print("-----------------------------------------")

        print("El porcentaje de nulos:", "\n")
        print((df.isnull().sum() / df.shape[0]) *  100)

    def limpiar_dataframe(self, df, lista_columnas): 

        #convertimos la fecha a datetime
        df["fecha"] = pd.to_datetime(df["fecha"])

        # reemplazamos los nulos de las columnas por la media
         # lista de columnas en las que queremos reemplazar los nulos
        df[lista_columnas]=df[lista_columnas].fillna(df.mean())

        # quitar columnas repetidas:
        
        df.drop(["ciudad_y"], axis = 1, inplace = True)
        
        # renombrar columnas
        
        df.rename(columns = {"ciudad_x": "ciudad"}, inplace = True)

        # quitar % 
        df["rh2m"] = df["rh2m"].replace(r"%", "", regex = True)

        # guardamos los datos una vez limpios
        df.to_pickle('datos/datos_Madrid.pkl')
        df.to_csv('datos/datos_Madrid.csv')

        return df
