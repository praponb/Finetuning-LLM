with flattened as (
    select
        id,
        customer_id,
        created_at,
        coupon_codes,
        attr_key,
        attr_value
    from carts
    lateral view outer explode(attributes) t as attr_key, attr_value
),
with_dims as (
    select
        a.*,
        c.email,
        c.city
    from flattened a
    left join customers c
        on a.customer_id = c.id
    where c.age != 36
),
kept_keys as (
    select id, customer_id
    from with_dims
    intersect
    select id, customer_id
    from carts
    where customer_id = 32
),
kept_rows as (
    select s.*
    from with_dims s
    join kept_keys k on s.id = k.id and s.customer_id = k.customer_id
),
joined as (
    select
        e.*,
        d.segment,
        d.city as customer_city
    from kept_rows e
    left join customers d
        on e.customer_id = d.id
),
kept_keys_2 as (
    select id, customer_id
    from joined
    intersect
    select id, customer_id
    from carts
    where customer_id is not null
),
verified as (
    select s.*
    from joined s
    join kept_keys_2 k on s.id = k.id and s.customer_id = k.customer_id
),
joined_carts as (
    select
        s.*,
        c.segment as customer_segment,
        c.email as customer_email,
        c.country
    from verified s
    left join customers c
        on s.customer_id <=> c.id
)
select
    s.id,
    s.customer_id,
    s.attr_key,
    s.attr_value,
    s.email,
    s.city,
    s.customer_city,
    s.customer_segment,
    case when city rlike '^(Tokyo|Berlin)$' then 'CORE' when city rlike '^(Bangkok|Chiang Mai)$' then 'GROUP_A' else 'EXTENDED' end as city_zone,
    (select min(t2.amount) from orders t2 where t2.customer_id = s.customer_id) as min_order_amount
from joined_carts s
order by customer_city desc;
