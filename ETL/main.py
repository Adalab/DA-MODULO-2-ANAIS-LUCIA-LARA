# Importamos las librerias

import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import mysql.connector
import ast
import requests


print("Se han importado correctamente las clases")

# Definición de clases

class Extraccion:
    
    # Constructor
    def __init__ (self, diccionario):
        
        self.diccionario = diccionario
    
    # Método para llamar a la API
    def llamar_API(self, producto, ruta):
        
        df_final= pd.DataFrame()

        for key, value in self.diccionario.items():
                
            url = f'http://www.7timer.info/bin/api.pl?lon=-{value[1]}&lat={value[0]}&product={producto}&output=json'
            response = requests.get(url)
            df= pd.DataFrame.from_dict(pd.json_normalize(response.json()["dataseries"]))
            df["latitud"] = value[0]
            df["longitud"] = value[1]
            df["paises"] = key
            df_final=pd.concat([df_final, df], axis= 0, ignore_index=True)
               
        df_final.to_csv(ruta)
        
print("Se ha definido correctamente la clase Extracción")


class Limpieza:    
   
    # Constructor
    
    def __init__(self, ruta1, ruta2): 
        
        self.dataframe1 = pd.read_csv(ruta1)
        self.dataframe2 = pd.read_csv(ruta2)
        
        
    # Método para poner en minúsculas los datos de las columnas categóricas 
       
    def minuscular(self, columna):
         
        self.dataframe2[columna]= self.dataframe2[columna].apply(lambda x:x.lower())
        
        return self.dataframe2
    
    
    # Método para filtrar por columnas según una lista de valores  
    def filtrar(self, columna, lista):
        
        self.dataframe1 = pd.DataFrame(self.dataframe1[self.dataframe1[columna].isin(lista)])  
       
        return self.dataframe1
    
    
    # Método para separar columnas con datos tipo json en distintas columnas  
    def separar(self, columna, valor1, valor2):   
        
        self.dataframe2[columna] = self.dataframe2[columna].apply(ast.literal_eval)

        x = self.dataframe2[columna].apply(pd.Series)      
        
        for values in range(len(x.columns)):
            nombre= valor1 + str(x[values].apply(pd.Series)[valor2][0])
            valores = list(x[values].apply(pd.Series)[valor1])
            self.dataframe2.insert(values, nombre, valores)
            
        return self.dataframe2
    
    # Método para group by    
    def agrupar_media(self, columna):
        
        self.dataframe2 = self.dataframe2.groupby(columna).mean().reset_index()
        
        return self.dataframe2
        
    
    # Método para unir dataframes          
    def unir(self, estrategia, columna1, columna2): 
        
        df = pd.merge(left= self.dataframe1, right = self.dataframe2, how= estrategia, left_on = columna1, right_on = columna2)
        
        return df

print("Se ha definido correctamente la clase Limpieza")

  
class Carga:
    
    # constructor
    def __init__(self, nombre_bbdd, contraseña):
        
        self.nombre_bbdd = nombre_bbdd
        self.contraseña = contraseña
        
        
    # método para crear la BBDD  
    def crear_bbdd(self):

        mydb = mysql.connector.connect(host="localhost",
                                       user="root",
                                       password=f'{self.contraseña}') 
        mycursor = mydb.cursor()
        print("Conexión realizada con éxito")

        try:
            mycursor.execute(f"CREATE DATABASE IF NOT EXISTS {self.nombre_bbdd};")
            
        except:
            print("La BBDD ya existe")
            
            
    # método para crear tablas  e insertar datos en ellas   
    def crear_insertar_tabla(self, query):
        
        mydb = mysql.connector.connect(host="localhost",
                                       user="root",
                                       password=f'{self.contraseña}', 
                                       database=f"{self.nombre_bbdd}") 
        mycursor = mydb.cursor()
        
        try:
            mycursor.execute(query)
            mydb.commit()
          
        except mysql.connector.Error as err:
            print(err)
            print("Error Code:", err.errno)
            print("SQLSTATE", err.sqlstate)
            print("Message", err.msg)
            

    # Método para ejecutar la inserción de datos en clima     
    def insertar_clima(self, dataframe):
        
        tabla_clima = """ CREATE TABLE IF NOT EXISTS clima (
            idclima INT NOT NULL AUTO_INCREMENT,
            timepoint INT NOT NULL,
            cloudcover INT NOT NULL,
            highcloud INT NOT NULL,
            midcloud  INT NOT NULL,
            lowcloud  INT NOT NULL,
            rh_profile VARCHAR(1000) NOT NULL,
            wind_profile VARCHAR(1000) NOT NULL,
            temp2m INT NOT NULL,
            lifted_index INT NOT NULL,
            rh2m INT NOT NULL,
            msl_pressure INT NOT NULL, 
            prec_type VARCHAR(255) NOT NULL,
            prec_amount INT NOT NULL,
            snow_depth INT NOT NULL,
            wind10m_direction INT NOT NULL,
            wind10m_speed INT NOT NULL,
            latitud FLOAT NOT NULL,
            longitud FLOAT NOT NULL,
            paises VARCHAR(255) NOT NULL,
            PRIMARY KEY (idclima))""" 
        
        self.crear_insertar_tabla(tabla_clima)
        
        for indice, fila in dataframe.iterrows(): # itreamos por el dataframe
        
            insert_clima = f"""            
                    INSERT INTO clima (timepoint, cloudcover, highcloud, midcloud, lowcloud, rh_profile, wind_profile, temp2m, lifted_index, rh2m, 
                    msl_pressure, prec_type, prec_amount, snow_depth, wind10m_direction, wind10m_speed, latitud, longitud, paises) 
                    VALUES ( "{fila["timepoint"]}","{fila['cloudcover']}", "{fila['highcloud']}", "{fila['midcloud']}", "{fila['lowcloud']}",
                    "{fila['rh_profile']}","{fila['wind_profile']}","{fila['temp2m']}","{fila['lifted_index']}","{fila['rh2m']}","{fila['msl_pressure']}"
                    ,"{fila['prec_type']}","{fila['prec_amount']}","{fila['snow_depth']}","{fila['wind10m.direction']}"
                    ,"{fila['wind10m.speed']}","{fila['latitud']}","{fila['longitud']}","{fila['paises']}");
                    """

            self.crear_insertar_tabla(insert_clima)
                        
    
    # Método para ejecutar la inserción de datos en ataques 
    def insertar_ataques(self, dataframe):
        
        tabla_ataques = """ CREATE TABLE IF NOT EXISTS ataques (
            case_number VARCHAR(255) NOT NULL,
            year INT NOT NULL,
            country VARCHAR(255),
            activity VARCHAR(255),
            species_ VARCHAR(255),
            fatal_ VARCHAR(255),
            sex2 VARCHAR(255) NOT NULL,  
            nuevas_edades FLOAT NOT NULL,
            PRIMARY KEY (case_number))"""
            
        self.crear_insertar_tabla(tabla_ataques)
        
        for indice, fila in dataframe.iterrows(): # itreamos por el dataframe
        
            insert_ataques = f"""            
                    INSERT INTO ataques (case_number, year, country, activity, species_, fatal_, sex2, nuevas_edades)
                    VALUES ( "{fila["case_number"]}","{fila['year']}","{fila['country']}","{fila['activity']}","{fila['species_']}","{fila['fatal_']}","{fila['sex2']}","{fila['nuevas_edades']}");
                    """
                    
            self.crear_insertar_tabla(insert_ataques) 
            
print("Se ha definido correctamente la clase Carga")

# Ejecución


## Ejecutando la clase Extracción se llama a una API para obtener información climática de los países que queramos (aportando su latitud y longitud)

diccionario = {"USA": [39.7837304, -100.445882], "Australia": [-24.7761086, 134.755], "South Africa": [-28.8166236, 24.991639], "New Zealand": [-41.5000831, 172.8344077], "Papua New Guinea": [-5.6816069, 144.2489081]}

instancia_extraccion = Extraccion(diccionario)

ruta2 = "../Ficheros/clima.csv"

instancia_extraccion.llamar_API("meteo", ruta2)

ruta1 = "../Ficheros/attacks_limpieza_completa.csv"

print("Se ha ejecutado correctamente la clase Extracción")


### Ejecntando la clase Limpieza se limpia el csv obtenido de la llamada a la API y se une con el csv de attack_limpieza_completa que nos facilitaba el enunciado.
### Este mergeo permite añadir al csv de attack_limpieza_completa filtrado por países las medias de las variables climáticas de cada país.

instancia_limpieza = Limpieza(ruta1, ruta2)

instancia_limpieza.minuscular("paises")

lista = ['usa', 'australia', 'south africa', 'new zealand','papua new guinea']

instancia_limpieza.filtrar("country", lista)

instancia_limpieza.separar("rh_profile", "rh", "layer")

instancia_limpieza.separar("wind_profile", "speed", "layer")

instancia_limpieza.agrupar_media("paises")

result = instancia_limpieza.unir("left", "country", "paises")

print("Se ha ejecutado correctamente la clase Limpieza")

### Para cargar en la base de datos se nos comenta que extraigamos información de dos csv diferentes:
### Por un lado, el csv sobre ataques limpio extraído de los ejercicios de pair de limpieza (no coincide con attack_limpieza completa que es facilitado en el gitbook). 
### Por otro, el csv de clima procedente de ETL 1 (que está sin limpiar, sin separar las columnas y sin hacer las medias)
### Por tanto, no se usa en esta parte el mergeado resultante de la clase de limpieza. 

df_ataques = pd.read_csv('../Ficheros/attack_limpieza5.csv', index_col= 0)
df_clima = pd.read_csv(ruta2, index_col= 0)

instancia_carga = Carga("tiburones", "AlumnaAdalab")

instancia_carga.crear_bbdd()

instancia_carga.insertar_clima(df_clima)

instancia_carga.insertar_ataques(df_ataques)

print("Se ha ejecutado correctamente la clase Carga")
