with lines as (
    select
        id,
        customer_id,
        created_at,
        coupon_codes,
        key,
        value
    from carts
    lateral view explode(attributes) x as key, value
)
select
    id,
    created_at,
    coupon_codes,
    key,
    first_value(key, true) over (partition by customer_id, key order by created_at, id, key rows between unbounded preceding and current row) as first_known_key,
    first_value(key, true) over (partition by customer_id order by created_at, id, key rows between unbounded preceding and current row) as first_known_key_2,
    first_value(key) ignore nulls over (order by created_at, id, key rows between unbounded preceding and current row) as first_known_key_3
from lines
where created_at < '2024-12-01';
