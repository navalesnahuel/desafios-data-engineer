from pyspark import SparkContext
from pyspark.sql import SparkSession

'''Crea la configuracion que usaremos en todos los trabajos de Spark (No es realmente necesario debido a que ya estan
dentro de lo que es el archivo spark-default.conf) pero de igual manera la creamos por si queremos correr los trabajos en otro cluster que no las tenga.'''

spark = SparkSession.builder.getOrCreate()

def load_config(spark_context: SparkContext):
    #S3 Conexion 
    spark_context._jsc.hadoopConfiguration().set('fs.s3a.access.key', 'admin')
    spark_context._jsc.hadoopConfiguration().set('fs.s3a.secret.key', 'adminpassword')
    spark_context._jsc.hadoopConfiguration().set('fs.s3a.path.style.access', 'true')
    spark_context._jsc.hadoopConfiguration().set('fs.s3a.endpoint', 'http://minio:9000')
    spark_context._jsc.hadoopConfiguration().set('fs.s3a.connection.ssl.enabled', 'false')
    spark_context._jsc.hadoopConfiguration().set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    
    # Hive Metastore URI
    spark_context._jsc.hadoopConfiguration().set("hive.metastore.uris", "thrift://hive-metastore:9083")

    # Delta Lake
    spark_context._conf.set('spark.sql.extensions', 'io.delta.sql.DeltaSparkSessionExtension')  
    spark_context._conf.set('spark.sql.catalog.spark_catalog', 'org.apache.spark.sql.delta.catalog.DeltaCatalog')  
    spark_context._conf.set("spark.databricks.delta.preview.enabled", "true") 
    spark_context._conf.set("spark.sql.catalogImplementation", "hive") 
    spark_context._conf.set('spark.sql.warehouse.dir', 's3a://data/warehouse')

# Carga la configuracion
load_config(spark.sparkContext)
