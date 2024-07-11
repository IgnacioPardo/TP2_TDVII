# Arquitectura y flujos de datos para MercadoPago

## Introducción

En este informe se presenta la arquitectura, los flujos de datos y los procesos de gobernanza para el modelo de MercadoPago. resumen de nuestro modelo?

### 1. Arquitectura

#### Origen de los datos

_¿Cuál es el origen de los datos? ¿Qué cadencia tienen, con qué formato arriban, qué volumen se espera?_

Los datos provienen principalmente de las acciones generadas por los usuarios. Dentro de las acciones consideramos abrir cuentas, realizar transacciones y depositar dinero a rendir. En nuestro caso no consideramos fuentes externas de datos dada la simplificación del modelo.

En el trabajo utilizamos la librería _Faker_ para simular las acciones de los usuarios.

La generación de datos es la siguiente:

Primero, se crean una cantidad específica de usuarios, cuentas bancarias y proveedores de servicios.
Luego, se simula el paso del tiempo durante un período determinado.

La simulación depende de los siguientes parámetros:

```{python}
num_users (int): Número de usuarios
num_cuentas_bancarias (int): Número de cuentas bancarias
num_servicios (int): Número de servicios
num_transacciones_sin_saldo (int): Número de transacciones a realizar en cuentas sin saldo
timespan (int): Duración de la simulación en días
```

Para cada día simulado:

- Se actualiza la fecha simulada al día correspondiente.
- Algunos usuarios comienzan inversiones en la fecha simulada.
- Algunos usuarios pagan por servicios utilizando las cuentas de los proveedores de servicios creados.
- Se pagan los rendimientos de las inversiones activas a los usuarios.
- Cada usuario recibe depósitos aleatorios en sus cuentas.
- Se realizan transacciones entre usuarios seleccionados al azar, manejando posibles errores como la falta de saldo. La cantidad de transacciones por día es aleatoria.
- Si han pasado más de 30 días, se realiza una predicción del volumen de transacciones y se actualizan las tasas de interés anuales. Esto luego se correría de forma diaria como parte del DAG de Airflow asi que tambien lo simulamos de forma diaria.

Todos los datos tienen una cadencia diaria _(no estoy seguro de esto)_, aunque las transacciones y rendimientos se generan a una tasa mucho mas alta. Los datos llegan en formato JSON a través de una API. Se espera un volumen elevado de datos, en particular de transacciones, cercano a 300 transacciones por segundo.

#### Linaje de los datos

_¿Cómo es el linaje de los datos desde su origen hasta que las distintas aplicaciones los consumen? ¿Cómo son los procesos intermedios y qué patrones siguen?_

1. Generación de datos: los datos se generan a partir de tres sistemas: procesamiento de pagos, usuarios y rendimientos. Estos sistemas son los que se comunican con el usuario final y procesan las acciones que éstos realizan.
2. Carga de Datos en Postgres: Los datos generados se cargan en una base de datos Postgres.
3. DAG de Airflow: Se ejecuta un DAG de Airflow que procesa los datos generados y los transforma para generar métricas, predicciones y reportes.
4. Transformaciones con DBT: Los datos cargados se transforman para obtener información útil y generar reportes.
5. Consumo por aplicación: Los datos transformados son consumidos por aplicaciones de análisis y reportes al igual que por la aplicación de los usuarios.

#### Usos de los datos en las distintas etapas

_¿Cuáles son los usos que tienen los datos en las distintas etapas de procesamiento?_

#### Gobernanza del dato

_¿Cómo es la gobernanza del dato? ¿Cuáles son los roles y qué tipos de permisos tienen sobre los datos?_

- Roles y permisos: Administrador de Datos: Acceso completo para gestión y mantenimiento de la base de datos.
- Analista de Datos: Acceso a datos transformados y reportes, capacidad para ejecutar queries.
- Usuario Final: Acceso a reportes y dashboards, con permisos de solo lectura.

### 2. Flujo de datos

#### Implementación del pipeline de carga

El pipeline de carga de datos está implementado en Airflow con el siguiente DAG:

[!dag.png]

A continuación se detalla la función de cada nodo:

##### data_generator

Simula una "ingesta" de datos en tiempo real, generando datos de usuarios, transacciones y rendimientos a partir de la librería Faker en un plazo de $n$ días (configurable en el operador). Estos datos se guardan en una base de datos Postgres modelada en el Trabajo Práctico 1.

##### transaction_volume_forecast

A partir de los datos ingestados, se computa el valor neto de dinero en la plataforma por día a partir de las transacciones que ingresaron dinero y las que lo retiraron. A partir de estos registros diarios, se fitea un modelo de _forecast_ (con `Prophet`) para las predicciones de transacción en los próximos 7 días. Luego, se calcula la variación porcentual de la predicción respecto al valor real de los últimos 7 días en promedio. Si la variación es mayor a un umbral, se actualiza la TNA de los rendimientos en la base de datos en función de la variación.

##### branch_ultimo_dia_mes

Un BranchDateTimeOperator determina si es el último día del mes utilizando `pendulum`. En caso de que sí, se ejecuta el operador _ultimo_dia_mes_, y en caso de que no, no se realiza nada.

##### ultimo_dia_mes

Genera un reporte mensual con algunas métricas sobre la aplicación en el último mes. Este reporte es utilizado internamente por varios equipos.

##### email_report

Una vez generado el reporte mensual, el mismo se envía por email a los directores de cada equipo.

justificación de la cadencia?

### 3. Enriquecimiento de Datos con DBT

Modelos DBT:

#### user_credit_card_usage

Este modelo analiza el uso de las tarjetas de crédito por parte de los usuarios. Incluye detalles sobre las transacciones realizadas con tarjetas de crédito, proporcionando una visión completa del comportamiento de uso de los usuarios.

#### high_usage_credit_card_users

Este modelo identifica a los usuarios que tienen un alto uso de sus tarjetas de crédito y crea una vista. Al analizar los patrones de uso, se puede determinar qué usuarios están utilizando intensivamente sus tarjetas y potencialmente necesitan atención o promociones especiales. Ademas podrían ser potenciales clientes de productos financieros como MercadoCrédito.

#### user_account_balances

Este modelo calcula y presenta los saldos de las cuentas de los usuarios. Incluye información sobre los saldos actuales, permitiendo el monitoreo y análisis del estado financiero de cada usuario en el sistema.

#### user_investment_performance_summary

Este modelo resume el rendimiento de las inversiones de los usuarios. Proporciona un análisis del desempeño de las inversiones, permitiendo a los usuarios y administradores del sistema evaluar la eficacia de las inversiones realizadas.

#### service_provider_transactions_summary

Este modelo proporciona un resumen de las transacciones realizadas a los proveedores de servicios. Incluye detalles sobre las transacciones y facilita el análisis del comportamiento financiero y operativo de los proveedores en el sistema.

#### Claves Uniformes (CBUs y CVUs)

A partir de los siguientes modelos:

- `claves_cards`: Consolida las claves únicas de las tarjetas de crédito.
- `claves_users`: Consolida las claves únicas de los usuarios.
- `claves_providers`: Consolida las claves únicas de los proveedores de servicios.
- `claves_rendimientos`: Consolida las claves únicas de los rendimientos de inversión.
- `claves_bank_accs`: Consolida las claves únicas de las cuentas bancarias.
- `claves_transactions`: Consolida las claves únicas de las transacciones.

El modelo `all_claves` consolida todas las claves del sistema en una única vista. Esto facilita la realización de análisis y verificaciones de integridad de los datos, asegurando que todas las claves cumplan con el formato esperado. Se verifica que las claves cumplan el formato de los CBUs y CVUs en Argentina.
