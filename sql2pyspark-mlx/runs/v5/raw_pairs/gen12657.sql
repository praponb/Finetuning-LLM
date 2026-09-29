with exploded as (
    -- Unnest the array into one row per element
    select
        id,
        plan,
        currency,
        explode_outer(tier_limits) as val
    from price_tiers
),
base as (
    -- Basic cleansing and defaults
    select *, length(plan) as plan_length
    from exploded
    where plan = 'basic'
),
remaining_keys as (
    -- Set difference on keys
    select id, plan
    from base
    except distinct
    select id, plan
    from price_tiers
    where effective_from < '2025-01-30'
),
verified as (
    -- Keep rows whose keys survive the set operation
    select s.*
    from base s
    join remaining_keys k on s.id = k.id and s.plan = k.plan
),
enriched as (
    -- Window metrics over several different partitions
    select
        id,
        currency,
        dense_rank() over (order by val desc) as val_dense_rank,
        cume_dist() over w_ordered as val_cume_dist,
        percent_rank() over (partition by currency order by plan_length desc nulls first) as plan_length_percent_rank,
        min(plan_length) over (partition by currency) as currency_min_plan_length,
        percent_rank() over w_grp as plan_length_percent_rank_2
    from verified
    window w_ordered as (partition by plan order by val desc nulls last), w_grp as (partition by currency order by plan_length)
),
base_price_tiers as (
    -- Filter the raw rows and derive helper columns
    select
        currency,
        val_cume_dist,
        currency_min_plan_length,
        plan_length_percent_rank_2,
        case when currency in ('THB', 'USD') then 'Tier 1' else 'Other' end as currency_group
    from enriched
    where currency = 'USD'
),
filtered as (
    -- Basic cleansing and defaults
    select *, lower(currency) as currency_lower
    from base_price_tiers
    where currency in ('EUR', 'THB', 'USD')
)
-- Native SQL UNPIVOT (wide to long)
select
    currency,
    metric_name,
    value
from (
    select
        currency,
        plan_length_percent_rank_2,
        val_cume_dist
    from filtered
) src
unpivot include nulls (
    value for metric_name in (plan_length_percent_rank_2, val_cume_dist)
)
