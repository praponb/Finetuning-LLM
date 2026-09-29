with lines as (
    select
        id,
        plan,
        currency,
        explode_outer(tier_prices) as tier_value
    from price_tiers
),
filtered as (
    select
        *,
        case when currency in ('THB', 'EUR') then 'Core' else 'Other' end as currency_group,
        substring(plan, 1, 2) as plan_prefix
    from lines
    where currency <> 'THB'
),
scored as (
    select
        id,
        plan,
        dense_rank() over (order by tier_value desc nulls last) as tier_value_dense_rank,
        percent_rank() over (order by tier_value nulls first) as tier_value_percent_rank,
        sum(tier_value) over (partition by currency, plan) as currency_sum_tier_value,
        stddev_samp(tier_value) over (partition by currency) as currency_stddev_samp_tier_value,
        max(tier_value) over (partition by plan, currency) as plan_max_tier_value
    from filtered
),
base as (
    select *, round(currency_sum_tier_value * 1.15, 2) as currency_sum_tier_value_adjusted
    from scored
    where plan = 'enterprise'
),
enriched as (
    select
        *,
        percent_rank() over (partition by plan order by plan_max_tier_value nulls first) as plan_max_tier_value_percent_rank,
        percent_rank() over (partition by plan order by currency_sum_tier_value_adjusted desc nulls last) as currency_sum_tier_value_adjusted_percent_rank,
        count(tier_value_percent_rank) over (partition by plan) as plan_count_tier_value_percent_rank,
        dense_rank() over (order by tier_value_dense_rank desc nulls last) as tier_value_dense_rank_dense_rank
    from base
)
select
    case when tier_value_dense_rank_dense_rank >= 8 then 'HIGH' else 'LOW' end as tier_value_dense_rank_dense_rank_tier,
    plan,
    avg(currency_sum_tier_value_adjusted_percent_rank) filter (where plan = 'starter') as starter_avg_currency_sum_tier_value_adjusted_percent_rank,
    round(sum(plan_max_tier_value) / nullif(count(case when plan = 'basic' then 1 end), 0), 2) as plan_max_tier_value_per_basic,
    sum(case when plan = 'pro' then tier_value_percent_rank else 0 end) as pro_tier_value_percent_rank
from enriched
group by tier_value_dense_rank_dense_rank_tier, plan
