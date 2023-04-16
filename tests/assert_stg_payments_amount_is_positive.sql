with payments as (
    select * from {{ source('stripe', 'payments') }}
)
select 
    order_id,
    sum(amount) as total_amount
from
    payments
group by 1
having total_amount < 0    
