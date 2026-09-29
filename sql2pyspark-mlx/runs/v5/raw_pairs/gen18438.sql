with flattened as (
    -- Unnest the array into one row per element
    select
        id,
        customer_id,
        created_at,
        coupon_codes,
        attr_key,
        value
    from carts
    lateral view outer explode(attributes) e as attr_key, value
),
enriched as (
    -- Attach customers attributes
    select src.*, c.email
    from flattened src
    join customers c
        on src.customer_id = c.id
),
with_dims as (
    -- Attach customers attributes
    select
        s.customer_id,
        s.created_at,
        s.coupon_codes,
        s.attr_key,
        s.value,
        s.email,
        d.city,
        d.credit_limit
    from enriched s
    join customers d
        on s.customer_id = d.id
),
with_windows as (
    -- Ranking and gap features
    select
        *,
        avg(credit_limit) over (partition by attr_key) as attr_key_avg_credit_limit,
        count(credit_limit) over (partition by city order by cast(created_at as bigint) range between 1800 preceding and current row) as credit_limit_count_30m,
        count(credit_limit) over (order by cast(created_at as bigint) range between 1800 preceding and current row) as credit_limit_count_30m_2,
        count(credit_limit) over (order by cast(created_at as bigint) range between 900 preceding and current row) as credit_limit_count_15m,
        percent_rank() over (partition by customer_id, attr_key order by credit_limit desc nulls first) as credit_limit_percent_rank
    from with_dims
),
valid as (
    -- Filter the raw rows and derive helper columns
    select *, concat(city, ' ', email) as city_email
    from with_windows
    where city is not null
      and attr_key is distinct from 'weight_kg'
),
stats as (
    -- Multi-level aggregation
    select
        customer_id,
        max(created_at) as last_seen,
        min(created_at) as first_seen,
        round(avg(credit_limit_count_15m), 2) as avg_credit_limit_count_15m,
        sum(attr_key_avg_credit_limit) filter (where city = 'Tokyo') as tokyo_total_attr_key_avg_credit_limit,
        count(*) filter (where attr_key = 'weight_kg') as weight_kg_rows
    from valid
    group by customer_id
)
select
    s.customer_id,
    s.first_seen,
    s.tokyo_total_attr_key_avg_credit_limit,
    s.weight_kg_rows,
    (select min(a2.balance) from accounts a2 where a2.customer_id = s.customer_id) as min_account_balance
from stats s
order by min_account_balance desc;
