{{ config(materialized='view') }}

WITH claves_bank_accs AS (
    SELECT
        clave_uniforme
    FROM
        CuentaBancaria
)
    
SELECT
    clave_uniforme
FROM
    claves_bank_accs
    