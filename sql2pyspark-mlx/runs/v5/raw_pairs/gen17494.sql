with lines as (
    -- Unnest the array into one row per element
    select
        id,
        customer_id,
        created_at,
        coupon_codes,
        key,
        value
    from carts
    lateral view explode(attributes) e as key, value
),
with_dims as (
    -- Attach customers attributes
    select a.*, b.credit_limit
    from lines a
    join customers b
        on a.customer_id <=> b.id
),
gaps as (
    -- Flag rows that start a new session (gap > interval)
    select *, case when created_at - lag(created_at) over (partition by key order by created_at, id, key) > interval 45 minutes then 1 else 0 end as is_new_session
    from with_dims
),
sessionized as (
    -- Running sum of the flag = session number
    select *, sum(is_new_session) over (partition by key order by created_at, id, key rows between unbounded preceding and current row) as session_id
    from gaps
),
joined as (
    -- Attach customers attributes
    select
        a.id,
        a.customer_id,
        a.created_at,
        a.key,
        a.credit_limit,
        a.session_id,
        d.city,
        d.age,
        d.credit_limit as customer_credit_limit
    from sessionized a
    left join customers d
        on a.customer_id <=> d.id
),
enriched as (
    -- Attach customers attributes
    select
        e.id,
        e.customer_id,
        e.key,
        e.credit_limit,
        e.session_id,
        b.city as customer_city
    from joined e
    join customers b
        on e.customer_id <=> b.id
),
matched_keys as (
    -- Set intersection on keys
    select customer_id, id
    from enriched
    intersect
    select customer_id, id
    from carts
    where created_at >= '2025-11-01'
),
kept_rows as (
    -- Keep rows whose keys survive the set operation
    select s.*
    from enriched s
    join matched_keys k on s.customer_id = k.customer_id and s.id = k.id
),
with_dims_carts as (
    -- Attach customers attributes
    select
        e.id,
        e.customer_id,
        e.key,
        e.credit_limit,
        p.city,
        p.email,
        p.age
    from kept_rows e
    left join customers p
        on e.customer_id = p.id
)
select
    s.customer_id,
    s.key,
    s.credit_limit,
    s.age,
    (select sum(o2.shipping_fee) from orders o2 where o2.customer_id = s.customer_id) as sum_order_shipping_fee
from with_dims_carts s
order by credit_limit nulls first, customer_id;
