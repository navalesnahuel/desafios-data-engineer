.PHONY: submit start restart stop status rebuild minio airflow spark jupyter sparkflow help
.SILENT: submit start restart stop status minio airflow spark jupyter rebuild sparkflow help

# Captura el primer argumento después del objetivo
file := $(word 2, $(MAKECMDGOALS))

submit:
	docker exec Spark-Master spark-submit /opt/spark/jobs/$(file)

start:
	docker compose up -d
restart:
	docker compose down && docker compose up -d
stop:
	docker compose down
status:
	docker ps
rebuild:
	docker compose down && docker compose up -d --build

# Prevenir que `make` trate los argumentos como objetivos
%:
	@:

minio:
	open http://localhost:9001
airflow:
	open http://localhost:8080
spark:
	open http://localhost:9090
jupyter:
	open http://localhost:8888

sparkflow:
	docker exec Airflow-Webserver airflow connections add 'spark_default' \
    --conn-type 'spark' \
    --conn-host 'spark://spark-master' \
    --conn-port '7077'

help:
	echo "Uso: make [COMMANDO]"
	echo ""
	echo "Commandos:"
	echo "  submit [file]      Envia trabajos hacia SparkMaster usando el archivo especificado."
	echo "  start              Inicia servicios usando docker-compose."
	echo "  restart            Reinicia servicios usando docker-compose."
	echo "  stop               Detiene servicios usando docker-compose."
	echo "  status             Muestra el estado de los contenedores Docker."
	echo "  rebuild            Reconstruye los servicios usando docker-compose."
	echo "  minio              Abre Minio en el navegador web (http://localhost:9001)."
	echo "  airflow            Abre Airflow en el navegador web (http://localhost:8080)."
	echo "  spark              Abre Spark en el navegador web (http://localhost:9090)."
	echo "  jupyter            Abre Jupyter en el navegador web (http://localhost:8888)."
	echo "  sparkflow          Crea conexion de Spark dentro de Airflow."
	echo "  help               Muestra esta ayuda."