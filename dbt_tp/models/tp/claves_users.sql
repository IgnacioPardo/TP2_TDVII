{{ config(materialized='view') }}

WITH claves_usuarios AS (
    SELECT
        clave_uniforme
    FROM
        Usuario
)
    
SELECT
    clave_uniforme
FROM
    claves_usuarios
    