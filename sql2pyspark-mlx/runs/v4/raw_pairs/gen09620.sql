with lines as (
    -- Unnest the array into one row per element
    select
        id,
        customer_id,
        attr_key,
        attr_value
    from carts
    lateral view outer explode(attributes) li as attr_key, attr_value
),
valid as (
    -- Filter the raw rows and derive helper columns
    select *, substring(attr_key, 1, 2) as attr_key_prefix
    from lines
    where customer_id is not null
),
joined as (
    -- Attach customers attributes
    select
        e.*,
        d.city,
        d.credit_limit
    from valid e
    left join customers d
        on e.customer_id = d.id
),
joined_carts as (
    -- Attach customers attributes
    select s.*, p.country
    from joined s
    left join customers p
        on s.customer_id = p.id
),
cleaned as (
    -- Filter the raw rows and derive helper columns
    select *, case when attr_key in ('device', 'weight_kg') then 'Group A' else 'Other' end as attr_key_group
    from joined_carts
    where country is not null
)
-- Grouped metrics
select
    case when credit_limit >= 13300 then 'PREMIUM' else 'STANDARD' end as credit_limit_tier,
    city,
    percentile_approx(credit_limit, 0.9) as credit_limit_p90,
    sum(credit_limit) as total_credit_limit,
    approx_percentile(credit_limit, 0.9) as credit_limit_p90_2
from cleaned
group by case when credit_limit >= 13300 then 'PREMIUM' else 'STANDARD' end, city
having credit_limit_p90 >= 0
order by credit_limit_tier desc
