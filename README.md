# Proyecto de Data Engineering

En este proyecto, aprenderás a realizar web scraping en la web de Mercadolibre, cargar los datos en un S3 creado con **MinIO** y luego, utilizando las funcionalidades de **Spark**, **Delta Lake** y **Airflow**, nos encargaremos de transformar, orquestar y gestionar los datos. Finalmente, emplearemos **Tableau** para crear un dashboard que muestre los resultados obtenidos.

El objetivo principal de este proyecto es abordar desafíos comunes en el rol de Data Engineer utilizando las tecnologías más avanzadas y prácticas disponibles en el ámbito de los datos, todo dentro de un entorno Dockerizado.

Además, quiero aclarar que dentro del mismo contenedor de Docker, están incluidos servicios como **Jupyter Notebook** o **Hive** (para la creación de Metastore). Estos nos pueden llegar a ser útiles si queremos realizar análisis más profundos con los datos obtenidos.

![Servicios Usados](/img/apps.png)

## Qué estamos construyendo y por qué?

La idea es construir un Lake House que contenga los datos de las principales marcas de celulares ofrecidos en Mercadolibre, preparados en formato medallion para ser utilizados en herramientas de visualización de la manera más eficiente posible. La pipeline se ejecutará diariamente para obtener actualizaciones de precios y poder rastrear cambios.

**¿Por qué este proyecto?** Siempre que entro en Mercadolibre, me encuentro con precios diferentes o precios cuya variación no es clara. Por eso, quise desarrollar una aplicación que permita conocer el último precio de un celular y si ha subido, bajado o se ha mantenido desde la última ejecución de la pipeline. Este proyecto es un ejemplo genuino que incluye una gran cantidad de desafíos típicos de un Data Engineer.

**¿Qué vamos a hacer?** Empezaremos con el web scraping, que nos permitirá tratar cualquier página web como una base de datos. Los datos extraídos se cargarán y almacenarán en nuestro S3 en la sección de Raw Data. A continuación, transformaremos y limpiaremos los datos para que sean utilizables mediante Spark. Con la ayuda de Delta Lake, fusionaremos la información para obtener las últimas actualizaciones de precios. Todo este proceso será orquestado por Airflow y se ejecutará en cualquier entorno utilizando Docker.

![Diagrama](/img/diagrama.png)

### Obteniendo la data - Scrapeando

Internet está lleno de una infinidad de información, por lo que al hacer scraping, es crucial identificar qué datos son valiosos y cuáles no lo son.

Para realizar un buen scraping en Python, hay dos librerías clave: Scrapy y BeautifulSoup. En este proyecto, preferí utilizar Scrapy por su comodidad y capacidad para manejar grandes volúmenes de datos de manera eficiente.

Para encontrar los elementos exactos que necesitamos extraer, usamos las herramientas de desarrollo del navegador. Presionando `F12` en la página de Mercadolibre, podemos inspeccionar el HTML y encontrar los selectores necesarios.

Esta técnica es esencial para identificar los selectores exactos que necesitamos en nuestros `XPath` para Scrapy. Con estas herramientas y técnicas, vas a poder extraer una gran cantidad de datos valiosos de la web de manera eficiente y estructurada.

### Almacenando en S3-MinIO

Una vez que tenemos establecida la conexión con MinIO, subir los datos scrapeados a nuestro almacenamiento S3 es un proceso fácil.

![Configuracion Spark-Minio](/img/config.png)

Con un código simple, podemos cargar los datos en el almacenamiento:

![Carga de Datos](/img/create_df.png)

### Añadiendo Funcionalidades de Base de Datos a S3 – Delta Lake y Spark

Para incorporar funcionalidades similares a las de una base de datos en tus archivos almacenados en S3, puedes crear una tabla Delta. Delta Lake facilita la gestión de esquemas dinámicos, permitiendo agregar nuevas columnas de manera incremental sin necesidad de dividir las ingestas en un lago de datos o canalizaciones posteriores.

¿Cómo crear o leer una tabla Delta? Se puede hacer fácilmente proporcionando el formato Delta:

![Carga de Datos en formato Delta](/img/delta_write.png)

Con Delta Lake, también puedes realizar operaciones avanzadas como merges, permitiéndote combinar datos de manera eficiente:

![Fusion de datos](/img/merge.png)

Usando estas funcionalidades y aplicando la limpieza y transformación de datos, podrás construir tu LakeHouse dentro de S3 con tablas Delta Lake.

### Orquestando con Airflow

Llegamos a la etapa final del proceso: la orquestación. Aunque existen varios orquestadores disponibles, en este proyecto utilicé Airflow. Con la creación de un DAG (Directed Acyclic Graph), me encargué de gestionar y coordinar los trabajos para que se envíen al clúster de Spark, garantizando que toda la data sea procesada correctamente y almacenada en S3.

### Visualizando los Datos con Tableau

Ningún proyecto está completo sin una visualización. Por eso, me tomé el tiempo para crear un dashboard sencillo en Tableau que presenta algunas visualizaciones clave para materializar nuestro trabajo y facilitar la interpretación de los datos.

![Dashboard](/img/dashboard.png)

### DevOps Engine - Docker

Finalmente, quiero destacar que todo el proyecto fue desarrollado utilizando Docker. El código incluye un Makefile que proporciona los comandos necesarios para ejecutar toda la aplicación en cualquier máquina que tenga Docker instalado.

Dentro del entorno Docker, también están disponibles los siguientes servicios:

- **Jupyter Notebooks**: Integrado con todos los servicios necesarios para realizar operaciones avanzadas con los datos.
- **Hive**: Utilizado como servicio de metastore.
- **PostgreSQL**: Para la gestión de datos relacionales.

Además de los servicios ya mencionados, como **Airflow**, **Spark** y **MinIO**.

## Conclusión

Para ser efectivo en el mundo de la ingeniería de datos, es crucial ser práctico con una amplia variedad de herramientas. Espero haber proporcionado inspiración para que puedas desarrollar tu propio proyecto de ingeniería de datos.

Eso es todo por ahora. Si te gustó, ¡me encantaría que dejaras una estrella en el repositorio de GitHub! 😉