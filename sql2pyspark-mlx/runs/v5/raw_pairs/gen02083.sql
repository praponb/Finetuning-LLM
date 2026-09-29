with base as (
    select
        id,
        order_id,
        carrier,
        delivered_date,
        cost,
        lower(carrier) as carrier_lower,
        length(status) as status_length,
        coalesce(carrier, 'NONE') as carrier_clean
    from shipments
    where shipped_date > to_date('2023-07-08')
),
joined as (
    select
        src.id,
        src.order_id,
        src.delivered_date,
        src.carrier_lower,
        src.carrier_clean,
        o.order_date,
        o.discount,
        o.amount
    from base src
    left join orders o
        on src.order_id <=> o.id
),
with_dims as (
    select
        src.id,
        src.order_id,
        src.carrier_clean,
        src.discount,
        b.shipping_fee,
        b.order_date as order_order_date
    from joined src
    inner join orders b
        on src.order_id = b.id
    where b.status = 'CANCELLED'
),
latest as (
    select *
    from with_dims
    qualify rank() over (partition by order_id order by order_order_date desc nulls first) = 1
)
select
    id,
    order_id,
    carrier_clean,
    discount,
    shipping_fee,
    order_order_date
from latest
order by order_id desc;
