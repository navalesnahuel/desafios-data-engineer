# Importaciones locales
from configs.configs_sparkbuilder import load_config
from configs.configs_paths import Paths
from configs.configs_logging import s3_logger

# Importaciones de bibliotecas
from pyspark.sql import SparkSession
from pyspark.sql.types import StringType, TimestampType, IntegerType, FloatType, DoubleType
from delta.tables import *
from delta.tables import DeltaTable
from delta import *


# Ejecutar configuraciones y obtener rutas
spark = SparkSession.builder.getOrCreate()  # Inicializa o obtiene la sesión de Spark
load_config(spark.sparkContext)  # Carga la configuración específica de Spark
paths = Paths()  # Instancia la clase Paths para manejar rutas de archivos

try:
    # Cargar los datos en un DataFrame desde la tabla Delta en la ruta de all_table
    df = spark.read.format("delta").load(paths.all_table_path)

    # Crear la tabla Delta updates si no existe
    DeltaTable.createIfNotExists() \
        .tableName("updates") \
        .addColumn("id", StringType(), nullable=True) \
        .addColumn("precio_antiguo", IntegerType(), nullable=True) \
        .addColumn("precio", IntegerType(), nullable=True) \
        .addColumn("variacion", DoubleType(), nullable=True) \
        .addColumn("marca", StringType(), nullable=True) \
        .addColumn("franquicia", StringType(), nullable=True) \
        .addColumn("modelo", StringType(), nullable=True) \
        .addColumn("status", StringType(), nullable=True) \
        .addColumn("fecha_de_entrada", TimestampType(), nullable=True) \
        .location(paths.updates_path) \
        .execute()

    # Crear una vista temporal con el DataFrame de all_table
    df.createOrReplaceTempView("updates")

    # Ejecuta una Query de SQL que permite actualizar los precios/data de la tabla all_table y permite una mas profundo analisis
    updates_query = spark.sql("""
        SELECT 
            id, 
            precio_antiguo,
            precio,
            ROUND(((precio - precio_antiguo) / precio_antiguo) * 100, 2) AS variacion, 
            marca,
            franquicia,
            modelo,
            CASE 
                WHEN precio_antiguo IS NULL THEN 'Nueva entrada'  
                ELSE 'Cambio de precio'  
            END AS status,
            ultima_modificacion as fecha_de_entrada  # Renombrar 'ultima_modificacion' a 'fecha_de_entrada'
        FROM updates
        WHERE precio_antiguo != precio 
        OR precio_antiguo IS NULL 
        ORDER BY status
    """)

    # Guardar la consulta en una tabla Delta en la ubicación de updates_path
    updates_query.write.format("delta").option("overwriteSchema", "true").mode("overwrite").save(paths.updates_path)

    s3_logger.info("la tabla 'Updates' fue creada/actualizada correctamente.")  # Loggea que la tabla de actualizaciones se actualizó correctamente

except Exception as e:
    s3_logger.error(f"Error: {str(e)}")  # Loggea cualquier error que ocurra durante el proceso