with flattened as (
    select
        id,
        customer_id,
        created_at,
        coupon_codes,
        key,
        attr_value
    from carts
    lateral view explode(attributes) x as key, attr_value
),
latest as (
    select *
    from flattened
    qualify row_number() over (partition by customer_id order by created_at desc, id, key) = 1
),
kept_keys as (
    select id, customer_id
    from latest
    intersect all
    select id, customer_id
    from carts
    where size(coupon_codes) > 1
),
kept_rows as (
    select s.*
    from latest s
    inner join kept_keys k on s.id = k.id and s.customer_id = k.customer_id
),
joined as (
    select
        src.customer_id,
        src.created_at,
        src.key,
        c.city,
        c.age,
        c.name
    from kept_rows src
    inner join customers c
        on src.customer_id <=> c.id
),
wide as (
    select *
    from (
        select
            customer_id,
            city,
            age
        from joined
    ) src
    pivot (
        avg(age) as value_sum,
        count(1) as n
        for city in ('London' as london, 'Berlin' as berlin, 'New York' as new_york)
    )
)
select
    s.customer_id,
    s.london_value_sum,
    s.london_n,
    s.berlin_n,
    s.new_york_value_sum,
    (select sum(x.shipping_fee) from orders x where x.customer_id = s.customer_id) as sum_order_shipping_fee
from wide s
order by customer_id desc
