FROM apache/airflow:2.7.1-python3.11

# Cambia al usuario root para ejecutar comandos de instalación
USER root

# Actualiza la lista de paquetes e instala gcc, python3-dev y openjdk-11-jdk, luego limpia la caché de apt
RUN apt-get update && \
    apt-get install -y gcc python3-dev openjdk-11-jdk && \
    apt-get clean

# Establece la variable de entorno JAVA_HOME para la ubicación de Java
ENV JAVA_HOME /usr/lib/jvm/java-11-openjdk-amd64

RUN mkdir -p /opt/airflow/jars && \
    chmod 755 /opt/airflow/jars

RUN curl -L https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.262/aws-java-sdk-bundle-1.12.262.jar -o /opt/airflow/jars/aws-java-sdk-bundle-1.12.262.jar && \
    curl -L https://repo1.maven.org/maven2/org/antlr/antlr4-runtime/4.9.3/antlr4-runtime-4.9.3.jar -o /opt/airflow/jars/antlr4-runtime-4.9.3.jar && \
    curl -L https://repo1.maven.org/maven2/io/delta/delta-contribs_2.12/3.1.0/delta-contribs_2.12-3.1.0.jar -o /opt/airflow/jars/delta-contribs_2.12-3.1.0.jar && \
    curl -L https://repo1.maven.org/maven2/io/delta/delta-iceberg_2.12/3.1.0/delta-iceberg_2.12-3.1.0.jar -o /opt/airflow/jars/delta-iceberg_2.12-3.1.0.jar && \
    curl -L https://repo1.maven.org/maven2/io/delta/delta-spark_2.12/3.1.0/delta-spark_2.12-3.1.0.jar -o /opt/airflow/jars/delta-spark_2.12-3.1.0.jar && \
    curl -L https://repo1.maven.org/maven2/io/delta/delta-storage/3.1.0/delta-storage-3.1.0.jar -o /opt/airflow/jars/delta-storage-3.1.0.jar && \
    curl -L https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar -o /opt/airflow/jars/hadoop-aws-3.3.4.jar

# Cambia al usuario airflow para ejecutar los siguientes comandos
USER airflow

# Instala Apache Airflow, el proveedor de Airflow para Apache Spark y PySpark
RUN pip install apache-airflow apache-airflow-providers-apache-spark pyspark

COPY Dockerfiles/conf/requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt