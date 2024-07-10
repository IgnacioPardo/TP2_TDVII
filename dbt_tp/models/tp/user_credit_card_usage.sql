{{ config(materialized='table') }}

with card_usage as (

    select 
        u.clave_uniforme,
        u.nombre,
        u.apellido,
        t.numero,
        count(tr.codigo) as total_transactions,
        sum(tr.monto) as total_spent
    from 
        Usuario u
    left join 
        Tarjeta t on u.clave_uniforme = t.CU
    left join 
        Transaccion tr on t.numero = tr.numero
    group by 
        u.clave_uniforme, u.nombre, u.apellido, t.numero

)

select *
from card_usage
where total_transactions > 0
