{{ config(materialized='table') }}

with performance_data as (

    select 
        ru.clave_uniforme,
        avg(r.TNA) as avg_tna,
        sum(r.monto) as total_investment,
        count(r.id) as total_rendimientos
    from 
        RendimientoUsuario ru
    left join 
        Rendimiento r on ru.id = r.id
    group by 
        ru.clave_uniforme

)

select *
from performance_data
where total_investment > 0
