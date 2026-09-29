with lines as (
    select
        id,
        customer_id,
        created_at,
        item.product_id,
        item.price,
        item.promo
    from (
        select
            id,
            customer_id,
            created_at,
            explode_outer(items) as item
        from carts
    ) e
)
select
    s.customer_id,
    s.created_at,
    (select min(o2.shipping_fee) from orders o2 where o2.customer_id = s.customer_id) as min_order_shipping_fee
from lines s
order by customer_id desc nulls last
