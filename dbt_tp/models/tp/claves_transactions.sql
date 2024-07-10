{{ config(materialized='view') }}

WITH claves_transactions AS (
    SELECT
        CU_Origen, CU_Destino
    FROM
        Transaccion
)
    
SELECT
    CU_Origen, CU_Destino
FROM
    claves_transactions
    