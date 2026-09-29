with flattened as (
    select
        id,
        customer_id,
        coupon_codes,
        attr_key,
        v
    from carts
    lateral view outer explode(attributes) x as attr_key, v
),
cleaned as (
    select *, size(coupon_codes) as coupon_codes_count
    from flattened
    where customer_id is not null
),
joined as (
    select
        e.customer_id,
        e.attr_key,
        e.v,
        e.coupon_codes_count,
        c.segment,
        c.city,
        c.name
    from cleaned e
    inner join customers c
        on e.customer_id = c.id
),
with_dims as (
    select
        s.customer_id,
        s.attr_key,
        s.v,
        s.city,
        s.name,
        p.signup_date
    from joined s
    left join customers p
        on s.customer_id <=> p.id
),
joined_carts as (
    select
        src.customer_id,
        src.attr_key,
        src.city,
        src.name,
        src.signup_date,
        d.city as customer_city,
        d.age
    from with_dims src
    inner join customers d
        on src.customer_id = d.id
)
select
    attr_key,
    city,
    name,
    customer_city
from joined_carts;
