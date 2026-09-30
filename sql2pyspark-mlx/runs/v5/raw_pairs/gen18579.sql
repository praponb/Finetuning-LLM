with lines as (
    -- Unnest the array into one row per element
    select
        id,
        customer_id,
        explode(coupon_codes) as element
    from carts
)
select
    s.id,
    s.customer_id,
    (select min(o2.discount) from orders o2 where o2.customer_id = s.customer_id) as min_order_discount
from lines s;
