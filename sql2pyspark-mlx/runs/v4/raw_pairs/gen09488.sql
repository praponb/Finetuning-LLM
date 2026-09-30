with lines as (
    -- Unnest the array into one row per element
    select
        id,
        plan,
        effective_from,
        currency,
        explode_outer(tier_limits) as tier_value
    from price_tiers
),
filtered as (
    -- Filter the raw rows and derive helper columns
    select *, concat(currency, ' - ', plan) as currency_plan
    from lines
    where currency in ('EUR', 'USD')
),
metrics as (
    -- Running and moving aggregates
    select
        *,
        rank() over (order by tier_value) as tier_value_rank,
        stddev_samp(tier_value) over (partition by currency) as currency_stddev_samp_tier_value
    from filtered
)
-- Aggregate with conditional metrics
select
    case when tier_value >= 70 then 'HIGH' else 'LOW' end as tier_value_tier,
    round(sum(currency_stddev_samp_tier_value) / nullif(count(case when plan = 'pro' then 1 end), 0), 2) as currency_stddev_samp_tier_value_per_pro,
    round(avg(tier_value), 2) as avg_tier_value
from metrics
group by case when tier_value >= 70 then 'HIGH' else 'LOW' end
order by tier_value_tier desc
