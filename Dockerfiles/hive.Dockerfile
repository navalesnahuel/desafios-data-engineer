FROM openjdk:8u342-jre

# Actualiza la lista de paquetes e instala telnet, luego limpia la caché
RUN apt-get update \
 && apt-get install --assume-yes telnet \
 && apt-get clean

# Actualiza la lista de paquetes e instala el cliente de PostgreSQL
RUN apt-get update && apt-get install -y postgresql-client

# Crea el directorio para la biblioteca de Hive Metastore
RUN mkdir -p /opt/apache-hive-metastore-${METASTORE_VERSION}-bin/lib/
WORKDIR /opt

# Establece las variables de entorno para las versiones de Hadoop y Hive Metastore
ENV HADOOP_VERSION=3.2.0
ENV METASTORE_VERSION=3.0.0
ARG POSTGRES_CONNECTOR_VERSION=42.2.23

# Define las variables de entorno para las rutas de instalación
ENV HADOOP_HOME=/opt/hadoop-${HADOOP_VERSION}
ENV HIVE_HOME=/opt/apache-hive-metastore-${METASTORE_VERSION}-bin

# Descarga e instala Hive Metastore y Hadoop, y el conector de PostgreSQL
RUN curl -L https://apache.org/dist/hive/hive-standalone-metastore-${METASTORE_VERSION}/hive-standalone-metastore-${METASTORE_VERSION}-bin.tar.gz | tar zx \
    && curl -L https://archive.apache.org/dist/hadoop/common/hadoop-${HADOOP_VERSION}/hadoop-${HADOOP_VERSION}.tar.gz | tar zx \
    && curl -L https://jdbc.postgresql.org/download/postgresql-${POSTGRES_CONNECTOR_VERSION}.jar -o postgresql-${POSTGRES_CONNECTOR_VERSION}.jar \
    && cp postgresql-${POSTGRES_CONNECTOR_VERSION}.jar /opt/apache-hive-metastore-${METASTORE_VERSION}-bin/lib/ \
    && rm postgresql-${POSTGRES_CONNECTOR_VERSION}.jar

# Copia el archivo de configuración de Hive y el script de entrada al contenedor
COPY Dockerfiles/conf/hive-site.xml ${HIVE_HOME}/conf
COPY Dockerfiles/conf/entrypoint.sh /entrypoint.sh

# Crea un grupo y un usuario para Hive, y establece permisos adecuados
RUN groupadd -r hive --gid=1000 && \
    useradd -r -g hive --uid=1000 -d ${HIVE_HOME} hive && \
    chown hive:hive -R ${HIVE_HOME} && \
    chown hive:hive /entrypoint.sh && chmod +x /entrypoint.sh

# Cambia al usuario Hive para ejecutar el contenedor
USER hive
# Expone el puerto 9083 para el servicio de Hive Metastore
EXPOSE 9083

# Define el punto de entrada del contenedor, ejecutando el script de entrada
ENTRYPOINT ["sh", "-c", "/entrypoint.sh"]
