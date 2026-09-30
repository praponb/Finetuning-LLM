with flattened as (
    -- Unnest the array into one row per element
    select
        id,
        customer_id,
        created_at,
        tag
    from carts
    lateral view explode(coupon_codes) li as tag
),
joined as (
    -- Attach customers attributes
    select
        src.id,
        src.customer_id,
        src.created_at,
        src.tag,
        c.age,
        c.credit_limit
    from flattened src
    join customers c
        on src.customer_id = c.id
),
valid as (
    -- Filter the raw rows and derive helper columns
    select *, case when age >= 66 then 'High' when age >= 28 then 'Medium' else 'Low' end as age_band
    from joined
    where age > 37
),
kept_keys as (
    -- Set intersection on keys
    select customer_id, id
    from valid
    intersect all
    select customer_id, id
    from carts
    where customer_id in (1, 21, 36)
),
verified as (
    -- Keep rows whose keys survive the set operation
    select s.*
    from valid s
    join kept_keys k on s.customer_id = k.customer_id and s.id = k.id
),
filtered as (
    -- Filter the raw rows and derive helper columns
    select
        *,
        substring(tag, 1, 3) as tag_prefix,
        hour(created_at) as created_at_hour
    from verified
    where created_at > timestamp '2025-03-01 16:00:00'
      and tag is distinct from 'SPRING10'
)
select
    s.id,
    s.customer_id,
    s.created_at,
    s.tag,
    s.age,
    s.credit_limit,
    (select avg(o2.discount) from orders o2 where o2.customer_id = s.customer_id) as avg_order_discount
from filtered s
order by credit_limit desc nulls last
