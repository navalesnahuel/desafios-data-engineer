# Importaciones locales
from configs.configs_sparkbuilder import load_config
from configs.configs_paths import Paths
from configs.configs_logging import s3_logger
from configs.configs_folders import get_files

# Importaciones de bibliotecas
from pyspark.sql.functions import lit
from pyspark.sql import SparkSession
from pyspark.sql.types import StringType, TimestampType, IntegerType
from delta.tables import *
from delta.tables import DeltaTable
from delta import *

# Ejecutar configuraciones y obtener rutas
spark = SparkSession.builder.getOrCreate()  # Inicializa o obtiene la sesión de Spark
load_config(spark.sparkContext)  # Carga la configuración específica de Spark
paths = Paths()  # Instancia la clase Paths para manejar rutas de archivos

# Itera sobre las marcas definidas en las rutas
for brand in paths.brands:
    try:
        brand_paths = paths.paths[brand]  # Obtiene las rutas específicas para la marca
        silver_files = get_files(brand_paths['silver_files'], brand)  # Obtiene los archivos de la capa silver para la marca
        table_path = f"{brand_paths['silver_path']}{silver_files}"  # Construye la ruta completa a los archivos silver

        # Cargar los datos en un DataFrame desde la tabla Delta en la ruta de la capa silver
        df = spark.read.format("delta").load(table_path)

        # Crear la tabla Delta si no existe
        DeltaTable.createIfNotExists() \
            .tableName("all_table") \
            .addColumn("id", StringType(), nullable=False) \
            .addColumn("precio_antiguo", IntegerType(), nullable=True) \
            .addColumn("precio", IntegerType(), nullable=False) \
            .addColumn("marca", StringType(), nullable=True) \
            .addColumn("franquicia", StringType(), nullable=True) \
            .addColumn("modelo", StringType(), nullable=True) \
            .addColumn("memoria", StringType(), nullable=True) \
            .addColumn("ram", StringType(), nullable=True) \
            .addColumn("condicion", StringType(), nullable=True) \
            .addColumn("envio", StringType(), nullable=True) \
            .addColumn("link", StringType(), nullable=True) \
            .addColumn("ultima_modificacion", TimestampType(), nullable=False) \
            .location(paths.all_table_path) \
            .execute()

        # Obtener referencia a la tabla Delta
        all_table = DeltaTable.forPath(spark, paths.all_table_path)

        # Realizar la operación de merge (actualización y inserción)
        all_table.alias("existing_data").merge(
            source=df.alias("new_data"),
            condition="existing_data.id = new_data.id"
        ).whenMatchedUpdate(set =
            {
                "existing_data.precio_antiguo": "existing_data.precio",
                "existing_data.precio": "new_data.precio",
                "existing_data.marca": "new_data.marca",
                "existing_data.franquicia": "new_data.franquicia",
                "existing_data.modelo": "new_data.modelo",
                "existing_data.memoria": "new_data.memoria",
                "existing_data.ram": "new_data.ram",
                "existing_data.condicion": "new_data.condicion",
                "existing_data.envio": "new_data.envio",
                "existing_data.link": "new_data.link",
                "existing_data.ultima_modificacion": "new_data.ultima_modificacion"
            }
        ).whenNotMatchedInsert(values =
            {
                "id": "new_data.id",
                "precio_antiguo": lit(None),  # Si no hay coincidencia, insertar con precio_antiguo como None
                "precio": "new_data.precio",
                "marca": "new_data.marca",
                "franquicia": "new_data.franquicia",
                "modelo": "new_data.modelo",
                "memoria": "new_data.memoria",
                "ram": "new_data.ram",
                "condicion": "new_data.condicion",
                "envio": "new_data.envio",
                "link": "new_data.link",
                "ultima_modificacion": "new_data.ultima_modificacion"
            }
        ).execute()

        s3_logger.info(f"La tabla 'all_table' fue creada/actualizada perfectamente con la data de {brand}.")  # Loggea que la tabla all_table se actualizó correctamente con la información de la marca

    except Exception as e:
        s3_logger.error(f"Error: {str(e)}")  # Loggea cualquier error que ocurra durante el proceso