with flattened as (
    select
        id,
        customer_id,
        created_at,
        coupon_codes,
        key,
        attr_value
    from carts
    lateral view outer explode(attributes) t as key, attr_value
)
select
    id,
    created_at,
    coupon_codes,
    key,
    attr_value
from flattened
order by created_at desc, id;
