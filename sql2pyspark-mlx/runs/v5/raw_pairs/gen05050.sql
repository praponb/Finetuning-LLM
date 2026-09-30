with valid as (
    -- Basic cleansing and defaults
    select *, coalesce(monthly_fee, 0) as monthly_fee_filled
    from subscriptions
    where customer_id is not null
),
latest as (
    -- Keep one row per key (latest first)
    select *, row_number() over (partition by customer_id order by start_date desc, id) as row_num
    from valid
    qualify row_num = 1
),
enriched as (
    -- Attach customers attributes
    select
        e.*,
        p.signup_date,
        p.email
    from latest e
    left join customers p
        on e.customer_id <=> p.id
),
with_dims as (
    -- Attach customers attributes
    select
        src.*,
        d.credit_limit,
        d.country
    from enriched src
    left join customers d
        on src.customer_id <=> d.id
)
-- Multi-level aggregation
select
    case when monthly_fee_filled >= 60 then 'PREMIUM' else 'STANDARD' end as monthly_fee_filled_tier,
    sort_array(collect_set(plan)) as plan_list,
    count(*) filter (where country = 'DE') as de_rows,
    sort_array(collect_set(plan)) as plan_list_2,
    count(distinct id) as distinct_ids,
    stddev_samp(monthly_fee) as monthly_fee_stddev,
    round(sum(credit_limit) / nullif(count(case when plan = 'premium' then 1 end), 0), 2) as credit_limit_per_premium
from with_dims
group by 1
order by monthly_fee_filled_tier desc;
