# Importaciones locales
from configs.configs_sparkbuilder import load_config
from configs.configs_paths import Paths
from configs.configs_logging import s3_logger

# Importaciones de bibliotecas
from pyspark.sql import SparkSession
from pyspark.sql.types import StringType
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

    # Crear la tabla Delta id_storage si no existe
    DeltaTable.createIfNotExists() \
        .tableName("id_storage") \
        .addColumn("id", StringType(), nullable=False) \
        .addColumn("link", StringType(), nullable=False) \
        .location(paths.id_storage_path) \
        .execute()

    # Obtener referencia a la tabla Delta id_storage
    ids = DeltaTable.forPath(spark, paths.id_storage_path)

    # Realizar la operación de merge (actualización y inserción)
    ids.alias("existing_data").merge(
        source=df.alias("new_data"),
        condition="existing_data.id = new_data.id"
    ).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()

    s3_logger.info("La tabla IDs fue cargada/actualizada correcatamente.")  # Loggea que la tabla id_storage se actualizó correctamente

except Exception as e:
    s3_logger.error(f"Error: {str(e)}")  # Loggea cualquier error que ocurra durante el proceso


