{{ config(materialized='table') }}

WITH all_claves AS (
    SELECT clave_uniforme AS clave FROM {{ ref('claves_users') }}
    UNION ALL
    SELECT clave_uniforme AS clave FROM {{ ref('claves_bank_accs') }}
    UNION ALL
    SELECT clave_uniforme AS clave FROM {{ ref('claves_providers') }}
    UNION ALL
    SELECT CU AS clave FROM {{ ref('claves_cards') }}
    UNION ALL
    SELECT CU_Origen AS clave FROM {{ ref('claves_transactions') }}
    UNION ALL
    SELECT CU_Destino AS clave FROM {{ ref('claves_transactions') }}
    UNION ALL
    SELECT clave_uniforme AS clave FROM {{ ref('claves_rendimientos') }}
)

SELECT clave
FROM all_claves
WHERE clave IS NOT NULL
