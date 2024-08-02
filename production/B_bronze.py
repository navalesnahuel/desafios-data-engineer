# Importaciones locales
from configs.configs_sparkbuilder import load_config 
from configs.configs_paths import Paths  
from configs.configs_logging import s3_logger 
from configs.configs_folders import get_files 

# Importaciones de bibliotecas
from pyspark.sql.functions import col, regexp_replace, when, ltrim, rtrim, from_unixtime
from pyspark.sql.types import StringType, TimestampType
from pyspark.sql import SparkSession 
from delta.tables import * 
from delta import * 

# Ejecutar configuraciones y obtener rutas
spark = SparkSession.builder.getOrCreate()  # Inicializa o obtiene la sesión de Spark
load_config(spark.sparkContext)  # Carga la configuración específica de Spark
paths = Paths()  # Instancia la clase Paths para manejar rutas de archivos

# Itera sobre las marcas definidas en las rutas
for brand in paths.brands:
    try:
        brand_paths = paths.paths[brand]  # Obtiene las rutas específicas para la marca
        raw_files = get_files(brand_paths['raw_files'], brand)  # Obtiene los archivos raw para la marca
        raw_path = f"{brand_paths['raw_path']}{raw_files}"  # Construye la ruta completa a los archivos raw

        # Lee los datos desde los archivos Parquet
        df = spark.read \
            .format("parquet") \
            .load(raw_path)

        # Ordenar el DataFrame
        df = df.select("id", "titulo", "precio", "marca", "franquicia", "modelo", "memoria", 
                        "ram", "condicion", "envio", "fingerprint", "link", "ultima_modificacion")

        # Eliminar errores de typo en los valores y cambiar valores
        df = df.withColumn("franquicia", regexp_replace(
                                            col("franquicia"), col("marca"), "").cast("string"))  \
                .withColumn("modelo", regexp_replace(
                                            col("modelo"), col("marca"), "").cast("string")) \
                .withColumn('precio', regexp_replace(
                                            'precio', '[,.]', '')) \
                .withColumn('envio', when(
                                            (col('envio') != 'Envío gratis') & 
                                            (col('envio') != 'Llega gratis') & 
                                            (col('envio') != 'Llega gratis mañana')
                                        , 'Pago') \
                                        .otherwise('Gratis'))

        # Recortar espacios en blanco en todas las columnas
        for column_name in df.columns:
            df = df.withColumn(column_name, ltrim(rtrim(col(column_name))))

        # Cambiar el tipo de las columnas del DataFrame
        df = df.withColumn('id', col('id').cast(StringType())) \
            .withColumn('titulo', col('titulo').cast(StringType())) \
            .withColumn('precio', col('precio').cast('int')) \
            .withColumn('envio', col('envio').cast(StringType())) \
            .withColumn('link', col('link').cast(StringType())) \
            .withColumn('marca', col('marca').cast(StringType())) \
            .withColumn('franquicia', col('franquicia').cast(StringType())) \
            .withColumn('modelo', col('modelo').cast(StringType())) \
            .withColumn('memoria', col('memoria').cast(StringType())) \
            .withColumn('ram', col('ram').cast(StringType())) \
            .withColumn('condicion', col('condicion').cast(StringType())) \
            .withColumn('fingerprint', col('fingerprint').cast(StringType())) \
            .withColumn("ultima_modificacion", from_unixtime(col("ultima_modificacion").cast("bigint")).cast(TimestampType()))

        # Escribir el DataFrame en formato Delta
        df.write.format("delta").mode("overwrite").save(brand_paths['bronze_path'])

        s3_logger.info(f"La tabla Bronze de {brand} fue creada exitosamente.") # Loggea que la carga de datos en la capa Bronze se completó exitosamente para la marca

    except Exception as e:
        s3_logger.error(f"Error: {str(e)}")  # Loggea cualquier error que ocurra durante el proceso