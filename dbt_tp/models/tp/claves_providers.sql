{{ config(materialized='view') }}

WITH claves_providers AS (
    SELECT
        clave_uniforme
    FROM
        ProveedorServicio
)
    
SELECT
    clave_uniforme
FROM
    claves_providers
    