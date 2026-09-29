with flattened as (
    -- Unnest the array into one row per element
    select
        id,
        plan,
        effective_from,
        currency,
        explode_outer(tier_prices) as tier_value
    from price_tiers
),
filtered as (
    -- Filter the raw rows and derive helper columns
    select
        id,
        plan,
        effective_from,
        tier_value,
        case when currency in ('USD', 'THB') then 'Group A' else 'Other' end as currency_group
    from flattened
    where effective_from > '2024-11-01'
),
filtered_price_tiers as (
    -- Filter the raw rows and derive helper columns
    select
        id,
        plan,
        effective_from,
        tier_value,
        currency_group,
        round(tier_value, 2) as tier_value_rounded,
        coalesce(tier_value, 0) as tier_value_filled
    from filtered
    where effective_from >= date '2024-06-06'
      and month(effective_from) in (2, 6, 9)
),
filtered_price_tiers_2 as (
    -- Basic cleansing and defaults
    select
        id,
        plan,
        effective_from,
        currency_group,
        tier_value_rounded,
        tier_value_filled,
        case when plan = 'enterprise' then 1 else 0 end as is_enterprise
    from filtered_price_tiers
    where tier_value >= 49
),
metrics as (
    -- Ranking and gap features
    select
        *,
        rank() over w_running as is_enterprise_rank,
        count(is_enterprise) over w_grp as plan_count_is_enterprise,
        max(tier_value_filled) over w_grp as plan_max_tier_value_filled,
        min(tier_value_filled) over w_grp as plan_min_tier_value_filled
    from filtered_price_tiers_2
    window w_running as (partition by plan order by is_enterprise nulls last), w_grp as (partition by plan)
)
select
    id,
    plan,
    effective_from,
    currency_group,
    tier_value_filled,
    is_enterprise_rank,
    plan_count_is_enterprise,
    plan_max_tier_value_filled,
    case when plan rlike '^(basic|pro)$' then 'CORE' when plan rlike '^(starter|enterprise)$' then 'EXTENDED' else 'GROUP_A' end as plan_zone
from metrics
order by id, plan_max_tier_value_filled desc;
