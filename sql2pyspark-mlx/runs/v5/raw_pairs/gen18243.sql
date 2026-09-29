with lines as (
    -- Unnest the array into one row per element
    select
        id,
        customer_id,
        coupon_codes,
        key,
        v
    from carts
    lateral view explode(attributes) t as key, v
)
select
    s.id,
    s.customer_id,
    s.coupon_codes,
    s.key,
    s.v,
    (select min(t2.discount) from orders t2 where t2.customer_id = s.customer_id) as min_order_discount
from lines s
order by id desc
