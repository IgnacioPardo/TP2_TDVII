{{ config(materialized='table') }}

with user_data as (

    select 
        u.clave_uniforme,
        u.nombre,
        u.apellido,
        u.saldo,
        u.fecha_alta,
        cb.banco
    from 
        Usuario u
    left join 
        CuentaBancaria cb on u.clave_uniforme = cb.clave_uniforme

)

select *
from user_data
where saldo > 0
