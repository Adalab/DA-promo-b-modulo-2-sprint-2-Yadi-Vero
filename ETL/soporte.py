
import requests
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import mysql.connector 

class Limpieza():
     
    def paises(producto):

        df_concat=pd.DataFrame()
        dict={'USA':[39.783730,-100.445882],'Australia':[-24.77610,134.755], 'South Africa':[-28.8166236,24.991639],\
            'New Zealand':[-41.5000831,172.8344077],'Papua New Guinea':[-5.6816069,144.2489081] }
            
        for k,v in dict.items():
                
            url = f'http://www.7timer.info/bin/api.pl?lon=-{v[1]}&lat={v[0]}&product={producto}&output=json'

            response = requests.get(url)
            df=pd.json_normalize(response.json()["dataseries"])
            df["latitud"] = v[0]
            df["longitud"] = v[1]
            df['paises']= k
            df_concat=pd.concat([df_concat,df], axis=0, ignore_index=True)

        return df_concat

    def limpiar_attack (df):

        df_attack=df[(df["country"]=="usa") | (df["country"]=="australia")|\
            (df["country"]=="south africa") | (df["country"]=="papua new guinea")|(df["country"]=="new zealand")]

        df_attack.rename(columns={'country':'id_paises'}, inplace=True)

        df_final=df_attack.groupby('id_paises')['year','age','latitud','longitud','fatal_N'].agg(['mean'])
        df_final=df_final.reset_index()

        return df_final
    
    def limpieza_clima(df):

        #Limpiamos la columna de rh_profile primero a series y luego a columnas en el bbucle for
        #df['rh_profile']=df['rh_profile'].apply(ast.literal_eval)
        rh = df['rh_profile'].apply(pd.Series)

        for i in range(len(rh.columns)):
            rh_col0=rh[i].apply(pd.Series)
            nombre="rh"+ rh_col0["layer"][115]
            valores= list(rh_col0["rh"])
            df.insert(i, nombre, valores)
            
        #Eliminamos la columna que ya no necesitamos
        df=df.drop(['rh_profile'], axis=1)
        
        #Hacemos lo mismo para la columnas wind_profile
        #df['wind_profile']=df['wind_profile'].apply(ast.literal_eval)
        wind = df['wind_profile'].apply(pd.Series)

        for i in range(len(wind.columns)):
            wind_col0=wind[i].apply(pd.Series)
            nombre="wind_"+ wind_col0["layer"][1]
            valores= list(wind_col0["direction"])
            df.insert(i, nombre, valores)
            
        df=df.drop(['wind_profile'], axis=1)
        
        df2=df.groupby('paises').agg(['mean'])
        df2=df2.reset_index()
        df2.rename(columns={'paises':'id_paises'}, inplace=True)
        def minuscula(col):
            try:
                col=col.lower()
                return col
            except:
                pass
        df2["id_paises"]=df2["id_paises"].apply(minuscula)

        return df2

    def merge(df_left, df_right): #Esta función está persanda para hacer un left. Para que se tenga en cuenta

        merge_final=pd.merge(left=df_left, right=df_right, how='left', left_on='id_paises', right_on='id_paises')
        merge_final.to_csv('merge_clima_ataques.csv')
        
        return merge_final


class Cargar_crear:
    
    def __init__(self,bbdd):
        
        self.bbdd = bbdd
    
    
    def crear_bbdd(self):

        mydb = mysql.connector.connect(host="localhost",
                                        user="root", 
                                        password="AlumnaAdalab")
        
        print("Conexión realizada con éxito")
        
        mycursor = mydb.cursor()

        try:
            mycursor.execute(f"CREATE DATABASE IF NOT EXISTS {self.bbdd};")
            print(mycursor)
        except mysql.connector.Error as err:
            print(err)
            print("Error Code:", err.errno)
            print("SQLSTATE", err.sqlstate)
            print("Message", err.msg)

    def crear_insertar_tabla(self,query):
    
        mydb = mysql.connector.connect(host="127.0.0.1",
                                    user="root",
                                    password="AlumnaAdalab", 
                                    database=f"{self.bbdd}") 
        mycursor = mydb.cursor()
        
        try:
            mycursor.execute(query)
            mydb.commit()
        
        except mysql.connector.Error as err:
            print(err)
            print("Error Code:", err.errno)
            print("SQLSTATE", err.sqlstate)
            print("Message", err.msg)
            

    def sacar_id_pais(self, pais):
    
        mydb = mysql.connector.connect(host="127.0.0.1",
                                    user="root",
                                    password="AlumnaAdalab", 
                                    database=f"{self.bbdd}") 
        mycursor = mydb.cursor()
        
        try:
            sacar_id = f"SELECT id FROM ataques WHERE pais = '{pais}'"
            mycursor.execute(sacar_id)
            id_paises = mycursor.fetchall()[0][0]
            return id_paises
        
        except: 
            return "No tenemos ese país en la BBDD y por lo tanto no te podemos sacar su ID"
        
    def insertar_datos(self,df):
        for i in df.columns:
            df[i]=df[i].astype("float64",errors="ignore")
        
        for indice, fila in df.iterrows(): # iteramos por el dataframe.

        # definimos nuestra query, igual que si lo hicieramos en workbench. ⚠️ Como estamos definiendo nuestra query en varias líneas usamos las triples comillas
        # lo valores que introduciremos serán los del dataframe que estamos iterando, por lo que usaremos los formats de los strings. 

            query_ataque = f"""
                INSERT INTO ataques (id_paises,year,age,latitud_x,longitud_x,fatal_N)
                VALUES ( "{fila["id_paises"]}", "{fila['year']}, "{fila['age']},"{fila['latitud_x']},"{fila['longitud_x']},"{fila['fatal_N']}");
                    """
        # una vez definida la query llamamos a la función que nos inserta los datos. 
            self.crear_insertar_tabla(query_ataque)
            
        for indice, fila in df.iterrows():

            id_paises=self.sacar_id_pais(fila['id_paises'])
    
            query_clima = f"""
                        INSERT INTO clima (wind_950mb,wind_900mb,wind_850mb,wind_800mb,wind_750mb,wind_700mb,wind_650mb,\
                            wind_600mb,wind_550mb,wind_500mb,wind_450mb,wind_400mb,wind_350mb,wind_300mb,wind_250mb,wind_200mb,\
                            rh950mb,rh900mb,rh850mb,rh800mb,rh750mb,rh700mb,rh650mb,rh600mb,rh550mb, rh500mb,rh450mb,rh400mb,rh350mb,\
                            rh300mb,rh250mb,rh200mb,timepoint,cloudcover,highcloud,midcloud,lowcloud,temp2m,lifted_index,rh2m,msl_pressure,\
                            prec_amount,snow_depth,wind10M.speed,latitud_y,longitud_y) 
                        VALUES ({fila['wind_950mb']},{fila['wind_900mb']},{fila['wind_850mb']},{fila['wind_800mb']},{fila['wind_750mb']}, 
                        {fila['wind_700mb']},{fila['wind_650mb']},{fila['wind_600mb']},{fila['wind_550mb']},{fila['wind_500mb']},{fila["wind_450mb"]}, 
                        {fila['wind_400mb']},{fila['wind_350mb']},{fila['wind_300mb']},{fila['wind_250mb']},{fila['wind_200mb']},{fila['rh950mb']},{fila['rh900mb']},
                        {fila["rh850mb"]},{fila['rh800mb']},{fila['rh750mb']},{fila['rh700mb']},{fila['rh650mb']},{fila['rh600mb']},{fila['rh550mb']},
                        {fila['rh500mb']},{fila["rh450mb"]},{fila['rh400mb']},{fila['rh350mb']},{fila['rh300mb']},{fila['rh250mb']},{fila['rh200mb']}, 
                        {fila['timepoint']},{fila['cloudcover']},{fila["highcloud"]},{fila['midcloud']},{fila['lowcloud']},{fila['temp2m']},{fila['lifted_index']},
                        {fila['rh2m']},{fila['msl_pressure']},{fila['prec_amount']},{fila['snow_depth']}, {fila['wind10m.speed']},{fila['latitud_y']},
                        {fila['longitud_y']},{id_paises});
                        """
                        
            self.crear_insertar_tabla(query_clima)
            # en vez de carga se pone el nombre de la variable que nosotros creemos cuando llamamos a 
            # la clase de crear tabla: carga.crear_insertar_tabla( query_medidas)
            