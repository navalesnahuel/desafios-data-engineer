import datetime
import yaml

class Paths:
    def __init__(self, settings_path="/opt/airflow/jobs/configs/settings.yaml"):
        """
        Inicializa la clase Paths, carga las configuraciones desde un archivo YAML,
        configura las rutas base para el almacenamiento de datos y establece las rutas
        específicas para cada marca.

        :param settings_path: Ruta del archivo de configuración YAML que contiene las configuraciones.
        """
        self.settings_path = settings_path
        self.load_settings()  # Cargar configuraciones del archivo YAML
        self.setup_paths()    # Configurar las rutas base para el almacenamiento
        self.set_brand_paths()  # Configurar rutas específicas para cada marca

    def load_settings(self):
        """
        Carga las configuraciones desde el archivo YAML especificado y establece
        los atributos de configuración necesarios.
        """
        # Leer el archivo YAML y cargar las configuraciones
        with open(self.settings_path, "r") as f:
            self.settings = yaml.safe_load(f)

        # Extraer configuraciones del archivo YAML
        self.url = self.settings['url']
        self.bucket = self.settings['bucket']
        self.raw_data = self.settings['raw_data_folder']
        self.brands = self.settings['brand']
        self.memory = self.settings['memory']
        self.ram = self.settings['ram']
        self.condition = self.settings['condition']
        self.current_datetime = datetime.datetime.now()  # Fecha y hora actual
        self.formatted_datetime = self.current_datetime.strftime("%Y-%m-%d_%H-%M-%S")  # Fecha y hora formateada

    def setup_paths(self):
        """
        Configura las rutas base para el almacenamiento de datos y logs en el bucket de S3.
        """
        # Rutas base para almacenamiento en el bucket de S3
        self.gold_path = f"s3a://{self.bucket}/gold/"
        # Rutas para tablas de datos
        self.all_table_path = f"{self.gold_path}all_table"
        self.id_storage_path = f"{self.gold_path}id_storage"
        self.updates_path = f"{self.gold_path}updates"
        # Rutas para logs y metadata
        self.logs = f"s3a://{self.bucket}/logs/"
        self.metadata = f"s3a://{self.bucket}/metadata/"

    def set_brand_paths(self):
        """
        Configura rutas específicas para cada marca, incluyendo rutas para datos en bruto y procesados.
        """
        self.paths = {}
        for brand in self.brands:
            # Generar un nombre de archivo basado en la marca y la fecha/hora actual
            file_name = f"{brand}_{self.formatted_datetime}"

            # Configurar rutas específicas para cada marca
            brand_paths = {
                'raw_path': f"s3a://{self.bucket}/{self.raw_data}/{brand}/",
                'bronze_path': f"s3a://{self.bucket}/bronze/{brand}/",
                'silver_path': f"s3a://{self.bucket}/silver/{brand}/",
                'write_raw': f"s3a://{self.bucket}/{self.raw_data}/{brand}/{file_name}",
                'write_silver': f"s3a://{self.bucket}/silver/{brand}/{file_name}",
                'raw_files': f"s3a://{self.bucket}/{self.raw_data}/{brand}/*",
                'silver_files': f"s3a://{self.bucket}/silver/{brand}/*/*"
            }
            # Almacenar las rutas configuradas en un diccionario con la marca como clave
            self.paths[brand] = brand_paths
