with exploded as (
    select
        id,
        plan,
        effective_from,
        currency,
        explode_outer(tier_prices) as tier_value
    from price_tiers
),
filtered as (
    select
        plan,
        effective_from,
        currency,
        tier_value,
        substring(currency, 1, 3) as currency_prefix
    from exploded
    where effective_from >= date '2024-04-29'
      and plan in ('starter', 'enterprise', 'basic')
),
cleaned as (
    select *, date_format(effective_from, 'yyyy-MM') as effective_from_label
    from filtered
    where plan in ('enterprise', 'pro', 'starter')
),
cleaned_price_tiers as (
    select
        plan,
        effective_from,
        currency,
        tier_value,
        currency_prefix,
        effective_from_label,
        initcap(trim(currency)) as currency_title,
        round(tier_value, 2) as tier_value_rounded
    from cleaned
    where effective_from between '2024-10-01' and '2025-01-01'
),
enriched as (
    select
        *,
        min(tier_value_rounded) over (partition by currency) as currency_min_tier_value_rounded,
        rank() over w_ordered as tier_value_rounded_rank,
        max(tier_value_rounded) over (partition by plan) as plan_max_tier_value_rounded
    from cleaned_price_tiers
    window w_ordered as (partition by plan order by tier_value_rounded desc nulls first)
)
select
    case when currency_min_tier_value_rounded >= 67 then 'LARGE' else 'SMALL' end as currency_min_tier_value_rounded_tier,
    case when tier_value_rounded_rank >= 8 then 'PREMIUM' else 'STANDARD' end as tier_value_rounded_rank_tier,
    approx_percentile(plan_max_tier_value_rounded, 0.95) as plan_max_tier_value_rounded_p95,
    count_if(plan = 'pro') as pro_events,
    sort_array(collect_set(currency)) as currency_list,
    sum(currency_min_tier_value_rounded) filter (where plan = 'enterprise') as enterprise_total_currency_min_tier_value_rounded
from enriched
group by case when currency_min_tier_value_rounded >= 67 then 'LARGE' else 'SMALL' end, case when tier_value_rounded_rank >= 8 then 'PREMIUM' else 'STANDARD' end
having sum(currency_min_tier_value_rounded) filter (where plan = 'enterprise') >= 0
