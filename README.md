# Airlow

ETL utilizando Airflow y Docker Compose.

<p>
<a href="https://airflow.apache.org/" rel="nofollow"><img src="https://cwiki.apache.org/confluence/download/attachments/145723561/airflow_transparent.png?api=v2" align="right" width="200" style="max-width: 100%;"></a>
</p>

# Repositorio

[Airflow](https://airflow.apache.org/) es una plataforma para crear, programar y monitorear flujos de trabajo mediante código. En otras palabras, es un orquestador de flujos de trabajo, que podremos utilizar para automatizar tareas.

Este repositorio contiene un codigo que ejectuta un ETL utilizando Apache Airflow y Docker Compose. 

El archivo *etl.py* se ejecuta dentro de *my_dag.py* generando un proceso de extraccion de datos, transformacion (limpieza y agregacion) y, por ultimo, carga de un .csv con salida a la carpeta home.


