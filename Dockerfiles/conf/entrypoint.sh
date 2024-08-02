#!/bin/sh

# entrypoint.sh configura y arranca el Metastore de Hive, asegurándose de que PostgreSQL esté disponible y que el esquema esté inicializado.

export HADOOP_HOME=/opt/hadoop-3.2.0
export HADOOP_CLASSPATH=${HADOOP_HOME}/share/hadoop/tools/lib/aws-java-sdk-bundle-1.11.375.jar:${HADOOP_HOME}/share/hadoop/tools/lib/hadoop-aws-3.2.0.jar
export JAVA_HOME=/usr/local/openjdk-8

# Asegurarse de que PostgreSQL esté listo
MAX_TRIES=8
CURRENT_TRY=1
SLEEP_BETWEEN_TRY=4
export PGPASSWORD=admin

until PGPASSWORD=$PGPASSWORD psql -h postgres -U admin -d metastore_db -c '\q' || [ "$CURRENT_TRY" -gt "$MAX_TRIES" ]; do
    echo "Waiting for PostgreSQL server..."
    sleep "$SLEEP_BETWEEN_TRY"
    CURRENT_TRY=$((CURRENT_TRY + 1))
done

if [ "$CURRENT_TRY" -gt "$MAX_TRIES" ]; then
  echo "WARNING: Timeout when waiting for PostgreSQL."
fi

# Verificar si el esquema existe
/opt/apache-hive-metastore-3.0.0-bin/bin/schematool -dbType postgres -info

if [ $? -eq 1 ]; then
  echo "Getting schema info failed. Probably not initialized. Initializing..."
  /opt/apache-hive-metastore-3.0.0-bin/bin/schematool -initSchema -dbType postgres
fi

/opt/apache-hive-metastore-3.0.0-bin/bin/start-metastore
