{{ config(materialized='table') }}

with provider_transactions as (

    select 
        ps.clave_uniforme as provider_id,
        ps.nombre_empresa,
        ps.categoria_servicio,
        count(t.codigo) as total_transactions,
        sum(t.monto) as total_amount
    from 
        ProveedorServicio ps
    left join 
        Transaccion t on ps.clave_uniforme = t.CU_Destino
    group by 
        ps.clave_uniforme, ps.nombre_empresa, ps.categoria_servicio

)

select *
from provider_transactions
where total_transactions > 0
