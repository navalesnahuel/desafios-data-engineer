FROM jupyter/base-notebook:x86_64-python-3.11

# Cambia al usuario root para poder instalar paquetes
USER root

# Instala Java, curl y otras dependencias necesarias
RUN apt-get update \
    && apt-get install -y openjdk-11-jdk curl \
    && apt-get clean

# Configura las variables de entorno para Java
ENV JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64
ENV PATH=$JAVA_HOME/bin:$PATH

# Descarga e instala Spark 3.5.1
RUN curl -L https://archive.apache.org/dist/spark/spark-3.5.1/spark-3.5.1-bin-hadoop3.tgz -o /tmp/spark-3.5.1-bin-hadoop3.tgz && \
    tar -xzf /tmp/spark-3.5.1-bin-hadoop3.tgz -C /usr/local && \
    mv /usr/local/spark-3.5.1-bin-hadoop3 /usr/local/spark && \
    rm /tmp/spark-3.5.1-bin-hadoop3.tgz

# Copia los archivos de configuración de Spark al directorio de configuración de Spark
COPY Dockerfiles/conf/core-site.xml /usr/local/spark/conf/core-site.xml
COPY Dockerfiles/conf/hive-site.xml /usr/local/spark/conf/hive-site.xml
COPY Dockerfiles/conf/spark-defaults.conf /usr/local/spark/conf/spark-defaults.conf
ENV SPARK_CONF_DIR=/usr/local/spark/conf

# Copia el archivo requirements.txt al directorio temporal y lo instala
COPY Dockerfiles/conf/requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt

# Copia el archivo sparkbuilder.txt al directorio de trabajo
COPY Dockerfiles/conf/sparkbuilder.txt /home/jovyan/work/sparkbuilder.txt

# Descarga los archivos JAR necesarios al directorio de JARs de Spark
RUN curl -L https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.262/aws-java-sdk-bundle-1.12.262.jar -o /usr/local/spark/jars/aws-java-sdk-bundle-1.12.262.jar && \
    curl -L https://repo1.maven.org/maven2/org/antlr/antlr4-runtime/4.9.3/antlr4-runtime-4.9.3.jar -o /usr/local/spark/jars/antlr4-runtime-4.9.3.jar && \
    curl -L https://repo1.maven.org/maven2/io/delta/delta-contribs_2.12/3.1.0/delta-contribs_2.12-3.1.0.jar -o /usr/local/spark/jars/delta-contribs_2.12-3.1.0.jar && \
    curl -L https://repo1.maven.org/maven2/io/delta/delta-iceberg_2.12/3.1.0/delta-iceberg_2.12-3.1.0.jar -o /usr/local/spark/jars/delta-iceberg_2.12-3.1.0.jar && \
    curl -L https://repo1.maven.org/maven2/io/delta/delta-spark_2.12/3.1.0/delta-spark_2.12-3.1.0.jar -o /usr/local/spark/jars/delta-spark_2.12-3.1.0.jar && \
    curl -L https://repo1.maven.org/maven2/io/delta/delta-storage/3.1.0/delta-storage-3.1.0.jar -o /usr/local/spark/jars/delta-storage-3.1.0.jar && \
    curl -L https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar -o /usr/local/spark/jars/hadoop-aws-3.3.4.jar

# Configura Jupyter para iniciar con la interfaz de Jupyter Lab
ENV JUPYTER_ENABLE_LAB=yes

# Expone el puerto 8888 para Jupyter Lab
EXPOSE 8888

# Cambia de nuevo al usuario del cuaderno
USER $NB_UID

# Comando para iniciar Jupyter Lab
CMD ["jupyter", "lab", "--ip=0.0.0.0", "--port=8888", "--no-browser", "--allow-root", "--NotebookApp.token="]