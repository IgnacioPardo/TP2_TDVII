{{ config(materialized='view') }}

WITH claves_cards AS (
    SELECT
        CU
    FROM
        Tarjeta
)
    
SELECT
    CU
FROM
    claves_cards
    