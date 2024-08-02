import datetime
from airflow.decorators import dag
from airflow.operators.empty import EmptyOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

@dag(start_date=datetime.datetime(2021, 7, 30), 
     dag_id='ml_pipeline',
     schedule_interval="@daily", 
     catchup=False)

# Creamos un dag que corre todos los trabajos de Spark de manera orquestada.

def sequential_spark_jobs():
    start = EmptyOperator(task_id='start')

    raw_scrapy = SparkSubmitOperator(
        task_id='raw',
        application='/opt/airflow/jobs/A_raw.py',
        conn_id='spark_default',
        conf={'spark.jars': 
            '''jars/hadoop-aws-3.3.4.jar,
            jars/antlr4-runtime-4.9.3.jar,
            jars/aws-java-sdk-bundle-1.12.262.jar, 
            jars/delta-contribs_2.12-3.1.0.jar, 
            jars/delta-iceberg_2.12-3.1.0.jar, 
            jars/delta-spark_2.12-3.1.0.jar, 
            jars/delta-storage-3.1.0.jar''',
        },
        verbose=True
    )

    bronze = SparkSubmitOperator(
        task_id='bronze',
        application='/opt/airflow/jobs/B_bronze.py',
        conn_id='spark_default',
        conf={'spark.jars': 
            '''jars/hadoop-aws-3.3.4.jar,
            jars/antlr4-runtime-4.9.3.jar,
            jars/aws-java-sdk-bundle-1.12.262.jar, 
            jars/delta-contribs_2.12-3.1.0.jar, 
            jars/delta-iceberg_2.12-3.1.0.jar, 
            jars/delta-spark_2.12-3.1.0.jar, 
            jars/delta-storage-3.1.0.jar''',
        },
       verbose=True
    )

    silver = SparkSubmitOperator(
        task_id='silver',
        application='/opt/airflow/jobs/C_silver.py',
        conn_id='spark_default',
                conf={'spark.jars': 
            '''jars/hadoop-aws-3.3.4.jar,
            jars/antlr4-runtime-4.9.3.jar,
            jars/aws-java-sdk-bundle-1.12.262.jar, 
            jars/delta-contribs_2.12-3.1.0.jar, 
            jars/delta-iceberg_2.12-3.1.0.jar, 
            jars/delta-spark_2.12-3.1.0.jar, 
            jars/delta-storage-3.1.0.jar'''
        },
        verbose=True
    )

    gold_all_table = SparkSubmitOperator(
        task_id='gold_all_table',
        application='/opt/airflow/jobs/D_gold_all_table.py',
        conn_id='spark_default',
                conf={'spark.jars': 
            '''jars/hadoop-aws-3.3.4.jar,
            jars/antlr4-runtime-4.9.3.jar,
            jars/aws-java-sdk-bundle-1.12.262.jar, 
            jars/delta-contribs_2.12-3.1.0.jar, 
            jars/delta-iceberg_2.12-3.1.0.jar, 
            jars/delta-spark_2.12-3.1.0.jar, 
            jars/delta-storage-3.1.0.jar'''
        },
        verbose=True
    )

    gold_ids_table = SparkSubmitOperator(
        task_id='gold_ids_table',
        application='/opt/airflow/jobs/E_gold_ids_table.py',
        conn_id='spark_default',
                conf={'spark.jars': 
            '''jars/hadoop-aws-3.3.4.jar,
            jars/antlr4-runtime-4.9.3.jar,
            jars/aws-java-sdk-bundle-1.12.262.jar, 
            jars/delta-contribs_2.12-3.1.0.jar, 
            jars/delta-iceberg_2.12-3.1.0.jar, 
            jars/delta-spark_2.12-3.1.0.jar, 
            jars/delta-storage-3.1.0.jar''',
        },
        verbose=True
    )

    gold_update_table = SparkSubmitOperator(
        task_id='gold_update_table',
        application='/opt/airflow/jobs/F_gold_update_table.py',
        conn_id='spark_default',
                conf={'spark.jars': 
            '''jars/hadoop-aws-3.3.4.jar,
            jars/antlr4-runtime-4.9.3.jar,
            jars/aws-java-sdk-bundle-1.12.262.jar, 
            jars/delta-contribs_2.12-3.1.0.jar, 
            jars/delta-iceberg_2.12-3.1.0.jar, 
            jars/delta-spark_2.12-3.1.0.jar, 
            jars/delta-storage-3.1.0.jar''',
        },
        verbose=True
    )

    end = EmptyOperator(task_id='end')

    start >> raw_scrapy >> bronze >> silver >> gold_all_table >> gold_ids_table >> gold_update_table >> end

sequential_spark_jobs()
