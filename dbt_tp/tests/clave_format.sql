SELECT clave
FROM {{ ref('all_claves') }}
WHERE clave IS NOT NULL
AND clave !~ '^0\d{1,3}0\d{1,4}\d{1}\d{13,14}$'
