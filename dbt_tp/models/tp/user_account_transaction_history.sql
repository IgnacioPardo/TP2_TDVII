{{ config(
    materialized='table'
) }}

WITH user_transactions AS (
    SELECT
        t.codigo AS transaction_id,
        t.CU_Origen AS user_id,
        t.monto AS amount,
        t.fecha AS date,
        t.descripcion AS description,
        t.estado AS status,
        t.es_con_tarjeta AS is_credit_card,
        t.numero AS card_number,
        t.interes AS interest,
        u.nombre AS user_name,
        u.apellido AS user_surname,
        u.email AS user_email
    FROM
        Transaccion t
    LEFT JOIN
        Usuario u
    ON
        t.CU_Origen = u.clave_uniforme
),

user_transactions_dest AS (
    SELECT
        t.codigo AS t2_transaction_id,
        t.CU_Destino AS t2_user_id,
        t.monto AS t2_amount,
        t.fecha AS t2_date,
        t.descripcion AS t2_description,
        t.estado AS t2_status,
        t.es_con_tarjeta AS t2_is_credit_card,
        t.numero AS t2_card_number,
        t.interes AS t2_interest,
        u.nombre AS t2_user_name,
        u.apellido AS t2_user_surname,
        u.email AS t2_user_email
    FROM
        Transaccion t
    LEFT JOIN
        Usuario u
    ON
        t.CU_Destino = u.clave_uniforme
)

SELECT * FROM user_transactions
JOIN user_transactions_dest
ON user_transactions.user_id = user_transactions_dest.t2_user_id
WHERE user_transactions.amount > 0
