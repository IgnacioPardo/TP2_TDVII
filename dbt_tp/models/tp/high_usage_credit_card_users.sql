{{ config(materialized='view') }}

select *
from {{ ref('user_credit_card_usage') }}
where total_spent > 500000
order by total_spent desc
limit 100
