# Arquitectura y flujos de datos para MercadoPago

## Introducción
En este informe se presenta la arquitectura, los flujos de datos y los procesos de gobernanza para el modelo de MercadoPago. resumen de nuestro modelo?

### 1. Arquitectura
#### Origen de los datos
_¿Cuál es el origen de los datos? ¿Qué cadencia tienen, con qué formato arriban, qué volumen se espera?_

Los datos provienen principalmente de las acciones generadas por los usuarios. Dentro de las acciones consideramos abrir cuentas, realizar transacciones y depositar dinero a rendir. En nuestro caso no consideramos fuentes externas de datos dada la simplificación del modelo. 
En el trabajo utilizamos la librería Faker para simular las acciones de los usuarios.  
Todos los datos tienen una cadencia diaria *(no estoy seguro de esto)*, aunque las transacciones y rendimientos se generan a una tasa mucho mas alta. Los datos llegan en formato JSON a través de una API. Se espera un volumen elevado de datos, en particular de transacciones, cercano a 300 transacciones por segundo. 

#### Linaje de los datos
_¿Cómo es el linaje de los datos desde su origen hasta que las
distintas aplicaciones los consumen? ¿Cómo son los procesos
intermedios y qué patrones siguen?_

1. Generación de datos: los datos se generan a partir de tres sistemas: procesamiento de pagos, usuarios y rendimientos. Estos sistemas son los que se comunican con el usuario final y procesan las acciones que éstos realizan.
2. Carga de Datos en Postgres*?*: Los datos generados se cargan en una base de datos Postgres mediante un pipeline orquestado por Airflow.
3. Transformaciones con DBT: Los datos cargados se transforman para obtener información útil y generar reportes.
4. Consumo por aplicación: Los datos transformados son consumidos por aplicaciones de análisis y reportes al igual que por la aplicación de los usuarios.

#### Usos de los datos en las distintas etapas
_¿Cuáles son los usos que tienen los datos en las distintas etapas de 
 procesamiento?_



#### Gobernanza del dato
_¿Cómo es la gobernanza del dato? ¿Cuáles son los roles y qué
tipos de permisos tienen sobre los datos?_

- Roles y permisos: Administrador de Datos: Acceso completo para gestión y mantenimiento de la base de datos.
- Analista de Datos: Acceso a datos transformados y reportes, capacidad para ejecutar queries.
- Usuario Final: Acceso a reportes y dashboards, con permisos de solo lectura.

### 2. Flujo de carga de datos
#### Implementación del pipeline de carga
El pipeline de carga de datos está implementado en Airflow con el siguiente DAG:

[!dag.png]

A continuación se detalla la función de cada nodo:

data_generator: carga los datos generados en la base de datos Postgres.

transaction_volume_forecast: se fitea un modelo de forecast (con Prophet) para las predicciones de transaccion en los proximos 7 dias. Estas predicciones son utilizadas por el equipo de finanzas.

branch_ultimo_dia_mes: un BranchDateTimeOperator que determina si es el último día del mes. En caso de que sí, se ejecuta _ultimo_dia_mes_, y en caso de que no, no se realiza nada. 

ultimo_dia_mes: genera un reporte mensual con algunas métricas sobre la aplicación en el último mes. Este reporte es utilizado internamente por varios equipos.

email_report: una vez generado el reporte mensual, el mismo se envía por email a los directores de cada equipo.

justificación de la cadencia?

### 3. Enriquecimiento de Datos con DBT

modelos dbt:

user_acount_balances: para uso en la app. Tabla ya que no cambian muy seguido.
user_investment_performance_summary: para uso en la app. Tabla
service_provider_transacion: para uso en la app?
user_credit_card_usage: ?
