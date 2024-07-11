# Trabajo Práctico 2 - TDVII - LTD - UTDT

## Integrantes

- Ignacio Pardo | 21R1160 | [ipardo@mail.utdt.edu](mailto:ipardo@mail.utdt.edu)
- Juan Ignacio Silvestri | 21Q111 | [jsilvestri@mail.utdt.edu](mailto:jsilvestri@mail.utdt.edu)

[Link al Informe](https://github.com/IgnacioPardo/TP2_TDVII/blob/027f507e57b7c34011382553b30ab076d7662d6c/informe.pdf)

## Estado

- [x] Generación de datos con Faker
- [x] Airflow
- [x] Proyecto DBT
- [x] DAG de generación de datos
- [x] DAG de transformación de datos con DBT
- [x] Informe

## Instrucciones para correr sin Docker

Hay dos formas para tener el ambiente de Python configurado:
1. Usando Poetry, es `poetry install` y luego `poetry shell` para activar el entorno. 

* Si no se tiene Poetry instalado, primero se puede [instalar pipx](https://pipx.pypa.io/stable/installation/) y luego instalando [poetry](https://python-poetry.org/docs/#installing-with-pipx).
* Si se prefiere usar el entorno local (o algún otro configurado con venv) se puede utilizar `pip3 install -r requirements.txt`.

Luego, para levantar Airflow se puede usar:

```
airflow db migrate && airflow users create --username airflow --firstname Peter --lastname Parker --role Admin --password airflow --email spiderman@superhero.org
airflow webserver --port 8080 & airflow scheduler &
```

## Instrucciones para correr con Docker

```
docker compose up -d
```

## Pasos para desarrollar el TP

1. Poner los statements para crear las tablas en sql/create_tables.sql.
2. Modificar las funciones de obtención de datos en td7/schema.py.
3. Escribir los generadores de datos en td7/data_generator.py, viendo también la lógica de generación en dags/fill_data.py.
4. Armar el o los DAGs necesarios en dags/.
    1. Un ejemplo de un nodo para cargar datos está en dags/fill_data.py.
    2. Un ejemplo de un nodo para correr transformaciones está en dags/run_dbt.py.
5. Armar las transformaciones de DBT usando el proyecto inicializado en `dbt_tp/`.
6. Para poder correr el DAG de DBT es necesario configurar una nueva conexión en Airflow: Admin > Connections > Add y luego configurar la conexión para el host `postgres` puerto `5432` con nombre `postgres` y configurar los parámetros de la conexión indicados en el `.env`.

Si quieren agregar dependencias pueden usar:

```
poetry add <dependencia>
poetry export --without-hashes --format=requirements.txt > requirements.txt
docker compose build
```

o directamente modificar el requirements.txt y correr el build de nuvo.

## Pasos para correr DBT a mano
```bash
docker compose run dbt <COMMAND>
```

## Ver documentación de DBT

En la UI de Airflow -> `Browse` --> `dbt Docs`
