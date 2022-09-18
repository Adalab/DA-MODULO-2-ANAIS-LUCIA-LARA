# DA-MODULO-2-ANAIS-LUCIA-LARA

Presentamos nuestra propuesta de ejercicios de este pair.

Nos gustaría especificar el hilo de archivos empleados durante la parte de ETL

Ejecutando la clase Extracción se llama a una API para obtener información climática de los países que queramos (aportando su latitud y longitud)

Ejecutando la clase Limpieza se limpia el csv obtenido de la llamada a la API y se une con el csv de attack_limpieza_completa que nos facilitaba el enunciado.
Este mergeo permite añadir al csv de attack_limpieza_completa filtrado por países las medias de las variables climáticas de cada país.

Al llegar a la ejecución de Carga:
Para cargar en la base de datos se nos comenta que extraigamos información de dos csv diferentes:
Por un lado, el csv sobre ataques limpio extraído de los ejercicios de pair de limpieza (no coincide con attack_limpieza completa que es facilitado en el gitbook).
Por otro, el csv de clima procedente de ETL 1 (que está sin limpiar, sin separar las columnas "rh_profile" y "wind_profile" y sin hacer las medias climáticas)
Por tanto, no se usa en esta parte el mergeado resultante de la clase de limpieza.

No sabemos si hay algún error, pero así es como lo hemos utilizado y entendido e introducido en la base de datos. 
