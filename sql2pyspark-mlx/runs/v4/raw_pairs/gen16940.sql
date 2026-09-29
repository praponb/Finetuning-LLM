with flattened as (
    select
        id,
        created_at,
        explode(coupon_codes) as value
    from carts
)
select id, value
from flattened
order by value;
