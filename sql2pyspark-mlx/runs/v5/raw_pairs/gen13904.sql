with exploded as (
    -- Unnest the array into one row per element
    select
        id,
        customer_id,
        k,
        value
    from carts
    lateral view outer explode(attributes) li as k, value
),
valid as (
    -- Keep recent, valid rows
    select *, substring(k, 1, 3) as k_prefix
    from exploded
    where k is not null
      and customer_id in (10, 14)
),
joined as (
    -- Attach customers attributes
    select
        a.id,
        a.customer_id,
        p.signup_date,
        p.segment
    from valid a
    left join customers p
        on a.customer_id = p.id
),
enriched as (
    -- Attach customers attributes
    select
        e.customer_id,
        e.signup_date,
        e.segment,
        d.credit_limit
    from joined e
    inner join customers d
        on e.customer_id = d.id
)
-- Join a dimension and aggregate
select
    p.country,
    a.segment as cart_segment,
    count(*) as num_rows,
    min(a.credit_limit) as min_credit_limit
from enriched a
inner join customers p on a.customer_id = p.id
group by p.country, a.segment;
