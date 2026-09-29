with exploded as (
    -- Unnest the array into one row per element
    select
        id,
        customer_id,
        explode_outer(coupon_codes) as tag
    from carts
),
joined as (
    -- Attach customers attributes
    select
        a.id,
        a.customer_id,
        p.credit_limit,
        p.city
    from exploded a
    left join customers p
        on a.customer_id = p.id
),
valid as (
    -- Keep recent, valid rows
    select
        *,
        coalesce(credit_limit, 0) as credit_limit_filled,
        coalesce(city, 'N/A') as city_filled
    from joined
    where city in ('Chiang Mai', 'Singapore')
),
filtered as (
    -- Keep recent, valid rows
    select
        *,
        cast(credit_limit as int) as credit_limit_int,
        coalesce(city, 'NONE') as city_clean
    from valid
    where city is not null
),
grouped as (
    -- Aggregate with conditional metrics
    select
        customer_id,
        city,
        count(*) as num_rows,
        sum(credit_limit_int) filter (where city = 'Singapore') as singapore_total_credit_limit_int,
        round(avg(credit_limit), 2) as avg_credit_limit,
        round(sum(credit_limit) / nullif(count(case when city = 'Tokyo' then 1 end), 0), 2) as credit_limit_per_tokyo,
        percentile_approx(credit_limit, 0.95) as credit_limit_p95,
        count_if(city = 'Tokyo') as tokyo_events
    from filtered
    group by customer_id, city
)
select
    customer_id,
    city,
    num_rows,
    credit_limit_per_tokyo,
    credit_limit_p95,
    tokyo_events
from grouped
order by customer_id;
