# Importaciones locales
from configs.configs_sparkbuilder import load_config 
from configs.configs_paths import Paths 
from configs.spider.ml_spider import MercadolibreCrawl 
from configs.configs_logging import s3_logger

# Importaciones de bibliotecas
from scrapy.crawler import CrawlerRunner 
from scrapy.utils.project import get_project_settings
from twisted.internet import reactor
from twisted.internet.defer import inlineCallbacks
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType

# Inicializa la sesión de Spark
spark = SparkSession.builder.getOrCreate()
load_config(spark.sparkContext)  # Carga la configuración específica de Spark
paths = Paths()  # Instancia la clase Paths para manejar rutas de archivos

# Define el esquema para el DataFrame de Spark
schema = StructType([
    StructField("id", StringType(), True),
    StructField("link", StringType(), True),
    StructField("titulo", StringType(), True),
    StructField("envio", StringType(), True),
    StructField("condicion", StringType(), True),
    StructField("precio", StringType(), True),
    StructField("memoria", StringType(), True),
    StructField("marca", StringType(), True),
    StructField("franquicia", StringType(), True),
    StructField("modelo", StringType(), True),
    StructField("ram", StringType(), True),
    StructField("fingerprint", StringType(), True),
    StructField("ultima_modificacion", StringType(), True)
])


# Función para ejecutar el proceso de scraping utilizando Scrapy y Twisted.
@inlineCallbacks
def run_crawler():
    runner = CrawlerRunner(get_project_settings())  # Inicializa el corredor de Scrapy con la configuración del proyecto

    # Itera sobre las marcas definidas en las rutas
    for brand in paths.brands:
        brand_paths = paths.paths[brand]  # Obtiene las rutas específicas para la marca

        # Define una clase personalizada del crawler para cada marca de forma dinámica
        class CustomMercadolibreCrawl(MercadolibreCrawl):
            start_urls = [
                paths.url.format(
                    condition=paths.condition,
                    brand=brand,
                    memory=paths.memory,
                    ram=paths.ram,
                    page_number=0
                )
            ]
            items = [] 

        # Programa la ejecución del crawler
        yield runner.crawl(CustomMercadolibreCrawl)

        # Obtiene los items scrapeados
        spider_instance = CustomMercadolibreCrawl()
        items = spider_instance.get_items_list()

        # Crea un DataFrame de Spark a partir de los ítems scrapeados
        new_listings = spark.createDataFrame(items, schema=schema)
        new_listings.coalesce(1).write.format("parquet").mode("overwrite").save(brand_paths['write_raw'])  # Guarda el DataFrame en formato Parquet
        s3_logger.info(f"Los archivos de {brand} fueron cargados correctamente.")  # Loggea que el scraping y escritura de datos se completaron exitosamente para la marca

    reactor.stop()  # Detiene el reactor de Twisted

def main():
#   Función principal para ejecutar el proceso de scraping.
    run_crawler()  # Ejecuta el crawler
    reactor.run()  # Inicia el reactor de Twisted

if __name__ == '__main__':
    main()  # Ejecuta la función principal si el script se corre directamente