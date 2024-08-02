import logging
import boto3
from botocore.exceptions import NoCredentialsError, ClientError

# Configuración del cliente de AWS/S3
s3_client = boto3.client(
    's3',
    aws_access_key_id="admin",  # ID de acceso para autenticación en el servidor S3 (MinIO)
    aws_secret_access_key="adminpassword",  # Contraseña secreta para autenticación
    endpoint_url="http://minio:9000",  # URL del servidor MinIO, debe reemplazarse con la URL de tu servidor MinIO
    use_ssl=False,  # Utilizar SSL (cambiar a True si se usa HTTPS)
)

class S3LogHandler(logging.Handler):
    def __init__(self, bucket_name, key_name):
        """
        Inicializa el manejador de logs para S3.

        :param bucket_name: Nombre del bucket en S3 donde se almacenarán los logs.
        :param key_name: Nombre del archivo en el bucket donde se almacenarán los logs.
        """
        super().__init__()
        self.bucket_name = bucket_name
        self.key_name = key_name
        # Configura el formato del log
        self.formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(filename)s - %(message)s')

    def emit(self, record):
        """
        Emite el log al bucket S3.

        :param record: Registro de log que se quiere emitir.
        """
        try:
            log_entry = self.format(record)  # Formatea el registro de log
            try:
                # Intenta obtener los logs existentes desde S3
                response = s3_client.get_object(Bucket=self.bucket_name, Key=self.key_name)
                existing_logs = response['Body'].read().decode('utf-8')
            except s3_client.exceptions.NoSuchKey:
                # Si el archivo de log no existe, inicializa los logs existentes como una cadena vacía
                existing_logs = ''

            # Agrega el nuevo log al existente
            new_logs = existing_logs + log_entry + '\n'
            # Sube los logs actualizados a S3
            s3_client.put_object(
                Bucket=self.bucket_name,
                Key=self.key_name,
                Body=new_logs.encode('utf-8'),
                ContentType='text/plain'
            )
        except NoCredentialsError:
            print("Credentials not available.")  # Manejo de excepción si las credenciales no están disponibles
        except ClientError as e:
            print(f"Error uploading log to S3: {e}")  # Manejo de errores al subir el log a S3

# Crear un logger para la salida en consola
console_logger = logging.getLogger('console')
console_logger.setLevel(logging.DEBUG)  # Configura el nivel de log a DEBUG
console_logger.addHandler(logging.StreamHandler())  # Agrega un manejador para imprimir en consola
console_logger.handlers[-1].setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(filename)s - %(message)s'))  # Configura el formato del log

# Crear un logger para la carga en S3
s3_logger = logging.getLogger('s3')
s3_logger.setLevel(logging.DEBUG)  # Configura el nivel de log a DEBUG
s3_logger.addHandler(S3LogHandler(bucket_name='data', key_name='logs/app.log'))  # Agrega el manejador personalizado para S3
