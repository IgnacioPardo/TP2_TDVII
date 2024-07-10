{{ config(materialized='view') }}

WITH claves_rendimientos AS (
    SELECT
        clave_uniforme
    FROM
        RendimientoUsuario
)
    
SELECT
    clave_uniforme
FROM
    claves_rendimientos
    