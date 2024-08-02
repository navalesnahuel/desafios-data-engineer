# Importaciones locales
from configs.configs_sparkbuilder import load_config
from configs.configs_paths import Paths
from configs.configs_logging import s3_logger

# Importaciones de bibliotecas
from pyspark.sql.functions import col
from pyspark.sql import SparkSession
from delta.tables import DeltaTable

# Ejecutar configuraciones y obtener rutas
spark = SparkSession.builder.getOrCreate()  # Inicializa o obtiene la sesión de Spark
load_config(spark.sparkContext)  # Carga la configuración específica de Spark
paths = Paths()  # Instancia la clase Paths para manejar rutas de archivos

# Itera sobre las marcas definidas en las rutas
for brand in paths.brands:
    try:
        brand_paths = paths.paths[brand]  # Obtiene las rutas específicas para la marca

        # Cargar los datos en un DataFrame desde la tabla Delta en la ruta de la capa bronze
        df = spark.read.format("delta").load(brand_paths['bronze_path'])

        # Filtrar el DataFrame para eliminar filas con valores nulos en las columnas especificadas
        df = df.filter(
            col("precio").isNotNull() & col("titulo").isNotNull() & col("id").isNotNull()
        )

        # Guardar la tabla Delta limpia en la ruta de la capa silver, particionada por "id"
        df.write.format("delta").mode("overwrite").partitionBy("id").save(brand_paths['write_silver'])

        # Optimizar el rendimiento de la tabla Delta
        delta_table = DeltaTable.forPath(spark, brand_paths['write_silver'])
        delta_table.optimize()  # Optimiza la tabla para mejorar el rendimiento de las consultas
        delta_table.vacuum()  # Elimina archivos obsoletos de la tabla para liberar espacio

        s3_logger.info(f"Los pasos de la capa Silver fueron completados perfectamente para la tabla de {brand}.")  # Loggea que los pasos de la capa silver se completaron exitosamente para la marca

    except Exception as e:
        s3_logger.error(f"Error: {str(e)}")  # Loggea cualquier error que ocurra durante el proceso