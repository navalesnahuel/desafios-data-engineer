from configs.configs_sparkbuilder import load_config
from pyspark.sql import SparkSession
from datetime import datetime

# Crear una sesión de Spark
spark = SparkSession.builder.getOrCreate()
# Cargar la configuración de Spark
load_config(spark.sparkContext)

def get_files(path, brand):
    """
    Obtiene el nombre del folder más reciente desde S3 basándose en los archivos listados.

    :param path: Ruta en S3 donde se encuentran los archivos.
    :param brand: Nombre de la marca que se usa para filtrar los folders.
    :return: El nombre del folder más reciente con base en la marca y el timestamp en su nombre.
    """
    # Listar archivos en la ruta especificada de S3
    file_list = spark.sparkContext.wholeTextFiles(path).map(lambda x: x[0]).collect()

    folders = set()  # Usar un conjunto para almacenar nombres de carpetas únicos
    # Extraer nombres de carpetas de las rutas de archivos
    for file_path in file_list:
        files_splited = file_path.split("/")
        if len(files_splited) == 8:  # Si la longitud de la ruta es 8
            folder = files_splited[-3]  # La carpeta es el tercer elemento desde el final
        else:
            folder = files_splited[-2]  # De lo contrario, la carpeta es el segundo elemento desde el final
        folders.add(folder)  # Añadir al conjunto para asegurar la unicidad

    # Filtrar carpetas para mantener solo aquellas con timestamps válidos
    valid_folders = []
    for folder in folders:
        try:
            # Intentar parsear el timestamp desde el nombre de la carpeta
            timestamp = datetime.strptime(folder.split(f'{brand}_')[1], "%Y-%m-%d_%H-%M-%S")
            valid_folders.append(folder)  # Añadir a las carpetas válidas
        except ValueError:
            pass  # Ignorar carpetas sin timestamps válidos

    if not valid_folders:
        raise ValueError("No se encontraron carpetas con timestamps válidos")

    # Obtener el índice del timestamp más reciente
    timestamps = [datetime.strptime(filename.split(f'{brand}_')[1], "%Y-%m-%d_%H-%M-%S") for filename in valid_folders]
    newest_index = timestamps.index(max(timestamps))  # Encontrar el índice del timestamp más reciente
    newest_folder = valid_folders[newest_index]  # Obtener el nombre de la carpeta más reciente

    return newest_folder

def list_brands(path):
    """
    Lista los nombres de las marcas encontradas en una ruta específica en S3.

    :param path: Ruta en S3 donde se encuentran los archivos.
    :return: Lista de nombres de carpetas (marcas) encontradas en la ruta.
    """
    # Obtener el estado de los archivos en la ruta especificada
    file_status = spark.sparkContext.wholeTextFiles(path + '*').map(lambda x: x[0]).collect()
    # Extraer y devolver los nombres de las carpetas (marcas) únicas
    folder_names = list(set([path.split('/')[-2] for path in file_status]))
    return folder_names

