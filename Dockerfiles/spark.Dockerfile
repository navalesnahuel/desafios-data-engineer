FROM bitnami/spark:3.5.1

# Cambia al usuario root para instalar paquetes necesarios
USER root

# Instala el paquete curl
RUN install_packages curl

# Copia el archivo requirements.txt al directorio temporal
COPY Dockerfiles/conf/requirements.txt /tmp/requirements.txt
# Instala los paquetes de Python especificados en requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt

# Copia la configuración predeterminada de Spark, core-site.xml y hive-site.xml al directorio de configuración de Spark
COPY Dockerfiles/conf/spark-defaults.conf /opt/bitnami/spark/conf/spark-defaults.conf
COPY Dockerfiles/conf/core-site.xml /opt/bitnami/spark/conf/core-site.xml
COPY Dockerfiles/conf/hive-site.xml /opt/bitnami/spark/conf/hive-site.xml

# Cambia al usuario con ID 1001 (usuario predeterminado de Bitnami)
USER 1001

# Descarga y coloca todos los JARS necesarios en el directorio de spark/jars
RUN curl https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.262/aws-java-sdk-bundle-1.12.262.jar --output /opt/bitnami/spark/jars/aws-java-sdk-bundle-1.12.262.jar
RUN curl https://repo1.maven.org/maven2/org/antlr/antlr4-runtime/4.9.3/antlr4-runtime-4.9.3.jar --output /opt/bitnami/spark/jars/antlr4-runtime-4.9.3.jar
RUN curl https://repo1.maven.org/maven2/io/delta/delta-contribs_2.12/3.1.0/delta-contribs_2.12-3.1.0.jar --output /opt/bitnami/spark/jars/delta-contribs_2.12-3.1.0.jar
RUN curl https://repo1.maven.org/maven2/io/delta/delta-iceberg_2.12/3.1.0/delta-iceberg_2.12-3.1.0.jar --output /opt/bitnami/spark/jars/delta-iceberg_2.12-3.1.0.jar
RUN curl https://repo1.maven.org/maven2/io/delta/delta-spark_2.12/3.1.0/delta-spark_2.12-3.1.0.jar --output /opt/bitnami/spark/jars/delta-spark_2.12-3.1.0.jar
RUN curl https://repo1.maven.org/maven2/io/delta/delta-storage/3.1.0/delta-storage-3.1.0.jar --output /opt/bitnami/spark/jars/delta-storage-3.1.0.jar
RUN curl https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar --output /opt/bitnami/spark/jars/hadoop-aws-3.3.4.jar
RUN curl https://repo1.maven.org/maven2/org/apache/spark/spark-hadoop-cloud_2.12/3.5.1/spark-hadoop-cloud_2.12-3.5.1.jar --output /opt/bitnami/spark/jars/spark-hadoop-cloud_2.12-3.5.1.jar
