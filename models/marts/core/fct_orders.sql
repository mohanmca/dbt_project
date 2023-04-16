with customers as (
 select * from {{ ref('stg_customers') }}
),
orders as (
    select * from {{ ref('stg_orders') }}
),
payments as (
    select * from {{ ref('stg_payments') }} p inner join orders o where o.order_id = p.ORDERID
),


final as (

    select
        customers.customer_id,
        orders.order_id,
        payments.amount
    from customers
        left join orders on (orders.user_id = customers.customer_id)
        left join payments on (payments.orderid = orders.orderid )

)

select * from final