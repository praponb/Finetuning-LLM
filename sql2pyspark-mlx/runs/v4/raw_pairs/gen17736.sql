with flattened as (
    -- Unnest the array into one row per element
    select
        id,
        plan,
        effective_from,
        currency,
        explode(discounts) as tier_value
    from price_tiers
),
enriched as (
    -- Window metrics over several different partitions
    select
        *,
        sum(tier_value) over w_ordered as plan_sum_tier_value,
        percent_rank() over (order by tier_value nulls last) as tier_value_percent_rank,
        avg(tier_value) over w_running as plan_avg_tier_value,
        avg(tier_value) over w_running as plan_avg_tier_value_2
    from flattened
    window w_ordered as (partition by plan, currency), w_running as (partition by plan)
),
base as (
    -- Filter the raw rows and derive helper columns
    select
        plan,
        currency,
        tier_value,
        plan_sum_tier_value,
        plan_avg_tier_value,
        datediff(current_date(), effective_from) as days_since_effective_from
    from enriched
    where effective_from >= date '2024-09-17'
),
enriched_price_tiers as (
    -- Window metrics over several different partitions
    select
        *,
        max(days_since_effective_from) over (partition by plan) as plan_max_days_since_effective_from,
        count(tier_value) over w_running as currency_count_tier_value,
        max(plan_sum_tier_value) over w_running as currency_max_plan_sum_tier_value
    from base
    window w_running as (partition by currency)
),
base_price_tiers as (
    -- Filter the raw rows and derive helper columns
    select
        plan,
        currency,
        plan_sum_tier_value,
        plan_avg_tier_value,
        days_since_effective_from,
        plan_max_days_since_effective_from,
        greatest(days_since_effective_from, plan_max_days_since_effective_from) as greatest_days_since_effective_from_plan_max_days_since_effective_from,
        plan_avg_tier_value * tier_value as plan_avg_tier_value_times_tier_value
    from enriched_price_tiers
    where plan_sum_tier_value between 186 and 236
),
filtered as (
    -- Filter the raw rows and derive helper columns
    select
        plan,
        currency,
        plan_sum_tier_value,
        plan_avg_tier_value,
        plan_max_days_since_effective_from,
        greatest_days_since_effective_from_plan_max_days_since_effective_from,
        plan_avg_tier_value_times_tier_value,
        upper(currency) as currency_upper
    from base_price_tiers
    where currency in ('USD', 'THB', 'EUR')
),
filtered_price_tiers as (
    -- Filter the raw rows and derive helper columns
    select
        *,
        plan_max_days_since_effective_from * plan_avg_tier_value as plan_max_days_since_effective_from_times_plan_avg_tier_value,
        case when plan_avg_tier_value_times_tier_value >= 83 then 'High' when plan_avg_tier_value_times_tier_value >= 20 then 'Medium' else 'Low' end as plan_avg_tier_value_times_tier_value_tier
    from filtered
    where plan = 'starter'
)
-- Grouped metrics
select
    case when plan_max_days_since_effective_from >= 74 then 'LARGE' else 'SMALL' end as plan_max_days_since_effective_from_tier,
    percentile_approx(plan_sum_tier_value, array(0.25, 0.5, 0.75)) as plan_sum_tier_value_quartiles,
    count(*) filter (where plan = 'enterprise') as enterprise_rows,
    sum(case when currency = 'THB' then plan_avg_tier_value else 0 end) as thb_plan_avg_tier_value,
    grouping(case when plan_max_days_since_effective_from >= 74 then 'LARGE' else 'SMALL' end) as grouping_level
from filtered_price_tiers
group by cube (case when plan_max_days_since_effective_from >= 74 then 'LARGE' else 'SMALL' end);
