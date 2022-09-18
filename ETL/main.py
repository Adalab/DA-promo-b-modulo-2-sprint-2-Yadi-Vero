
import requests
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import mysql.connector   

#Vamos a cargarnos el archivo de etl 1
print("Hola! Vamos a iniciar el proceso de limpieza!")
ruta_ataques=input("Escribe la ruta absoluta de tu archivo de ataques")
ruta_paises=input("Escribe la ruta absoluta de tu archivo api paises")
attack=pd.read_csv(f'{ruta_ataques}', index_col=0)
df=pd.read_csv(f'{ruta_paises}', index_col=0)
print("csv's cargado's") 

import soporte as sp

prueba=sp.Limpieza()

df_attack=sp.Limpieza.limpiar_attack(attack)
print("Tu archivo de ataques ha sido limpiado ahora se llama df_attack")

producto = input("¿Indica el producto que quieres?  ")
print("---------------------------------------------------------------")

df_api=sp.Limpieza.paises(producto)

print("Se ha generado tu archivo con el producto que especificaste")
print("---------------------------------------------------------------")

df_clima=sp.Limpieza.limpieza_clima(df_api)
print("Se ha limpiado el archivo que se había generado")
print("---------------------------------------------------------------")

df_final=sp.Limpieza.merge(df_attack,df_clima)
df_final=df_final.drop(["wind10m.direction"], axis=1)
df_final.dropna(axis=0, how="any", subset=None,inplace= True)
print("Se han mergeado los 2 archivos que se habían generado, el cual se llama df_final")
print("---------------------------------------------------------------")

nombre= input("Vamos a guardar tu archivo escribe la ruta, el nombre y la extención que quieras")
df_final.to_csv(f"{nombre}")

print("Se ha guardado el archivo")

print("Genial vamos con el siguiente paso ya falta menos")
df=pd.read_csv(f'{nombre}',index_col=0)
df.dropna(axis=0, how="any", subset=None,inplace= True)


tabla_ataques= '''
CREATE TABLE IF NOT EXISTS `tiburones`.`ataques` (
  `id` INT NOT NULL AUTO_INCREMENT,
  `id_paises` VARCHAR (45) NOT NULL,
  `year` INT NOT NULL, 
  `age` INT NOT NULL,
  `latitud_x` INT NOT NULL,
  `longitud_x` INT NOT NULL,
  `fatal_N` INT NOT NULL,
  PRIMARY KEY (`id`))
ENGINE = InnoDB;
'''

tabla_clima= '''
CREATE TABLE IF NOT EXISTS `tiburones`.`clima` (
  `id_clima` INT NOT NULL AUTO_INCREMENT,
  `id` INT NOT NULL,
  `wind_950mb` INT NOT NULL,
  `wind_900mb` INT NOT NULL, 
  `wind_850mb` INT NOT NULL, 
  `wind_800mb` INT NOT NULL,
  `wind_750mb` INT NOT NULL, 
  `wind_700mb` INT NOT NULL, 
  `wind_650mb` INT NOT NULL, 
  `wind_600mb` INT NOT NULL, 
  `wind_550mb` INT NOT NULL,
  `wind_500mb` INT NOT NULL, 
  `wind_450mb` INT NOT NULL, 
  `wind_400mb` INT NOT NULL, 
  `wind_350mb` INT NOT NULL, 
  `wind_300mb` INT NOT NULL,
  `wind_250mb` INT NOT NULL, 
  `wind_200mb` INT NOT NULL, 
  `rh950mb` INT NOT NULL, 
  `rh900mb` INT NOT NULL, 
  `rh850mb` INT NOT NULL, 
  `rh800mb` INT NOT NULL,
  `rh750mb` INT NOT NULL, 
  `rh700mb` INT NOT NULL, 
  `rh650mb` INT NOT NULL, 
  `rh600mb` INT NOT NULL, 
  `rh550mb` INT NOT NULL, 
  `rh500mb` INT NOT NULL,
  `rh450mb` INT NOT NULL, 
  `rh400mb` INT NOT NULL, 
  `rh350mb` INT NOT NULL, 
  `rh300mb` INT NOT NULL, 
  `rh250mb` INT NOT NULL, 
  `rh200mb` INT NOT NULL,
  `timepoint` INT NOT NULL, 
  `cloudcover` INT NOT NULL,
  `highcloud`INT NOT NULL,
  `midcloud` INT NOT NULL,
  `lowcloud`INT NOT NULL,
  `temp2m` INT NOT NULL,
  `lifted_index` INT NOT NULL,
  `rh2m` INT NOT NULL,
  `msl_pressure` INT NOT NULL,
  `prec_amount` INT NOT NULL,
  `snow_depth` INT NOT NULL,
  `wind10M.speed` INT NOT NULL,
  `latitud_y` INT NOT NULL,
  `longitud_y` INT NOT NULL,
  PRIMARY KEY (`id_clima`),
  CONSTRAINT `fk_tabla_clima_tabla_ataques`
    FOREIGN KEY (`id`)
    REFERENCES `tiburones`.`ataques` (`id`))
ENGINE = InnoDB;
'''


bbdd=input("Escribe el nombre de la base de datos que quieres crear/usar")

probando=sp.Cargar_crear(bbdd)

crear_bbdd=probando.crear_bbdd()

print("Genial! Se ha creado/conectado tu base de datos con éxito")

crear_tabla_ataque=probando.crear_insertar_tabla(tabla_ataques)

print("Genial! Se ha creado la tabla con éxito")

crear_tabla_clima=probando.crear_insertar_tabla(tabla_clima)

print("Genial! Se ha creado la tabla con éxito")

print("Estamos insertando tus datos en las tablas (si tan sólo pudieramos...)")

insertar=probando.insertar_datos(df)


print("Genial! Hemos terminado, buen día!")



