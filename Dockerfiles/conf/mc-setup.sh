#!/bin/sh

# mc-setup.sh Se encarga de crear el Bucket Data en el servidor de MinIO.

# Establece el alias del servidor MinIO
/usr/bin/mc alias set myminio http://minio:9000 admin adminpassword

# Verifica si el bucket "data" existe y, si no existe, lo crea
if ! /usr/bin/mc ls myminio/data > /dev/null 2>&1; then
  echo 'Bucket does not exist. Creating bucket...'
  /usr/bin/mc mb myminio/data
  /usr/bin/mc policy set download myminio/data
else
  echo 'Bucket already exists.'
fi

# Finaliza el script con un código de salida 0 (éxito)
exit 0
