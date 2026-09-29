with exploded as (
    -- Unnest the array into one row per element
    select id, tier_value
    from price_tiers
    lateral view explode(discounts) x as tier_value
),
valid as (
    -- Filter the raw rows and derive helper columns
    select
        *,
        case when tier_value >= 67 then 'High' when tier_value >= 19 then 'Medium' else 'Low' end as tier_value_level,
        round(tier_value, 0) as tier_value_rounded
    from exploded
    where tier_value <= 63
),
remaining_keys as (
    -- Set difference on keys
    select id
    from valid
    except
    select id
    from price_tiers
    where currency in ('USD', 'THB', 'EUR')
),
kept_rows as (
    -- Keep rows whose keys survive the set operation
    select s.*
    from valid s
    inner join remaining_keys k on s.id = k.id
),
with_windows as (
    -- Window metrics over several different partitions
    select
        tier_value,
        tier_value_rounded,
        rank() over (order by tier_value desc nulls last) as tier_value_rank,
        rank() over (order by tier_value desc nulls last) as tier_value_rank_2
    from kept_rows
    where tier_value_rounded <= 65
),
valid_price_tiers as (
    -- Basic cleansing and defaults
    select *, round(tier_value_rounded, 2) as tier_value_rounded_rounded
    from with_windows
    where tier_value_rounded is not null
),
cleaned as (
    -- Basic cleansing and defaults
    select *, tier_value_rank + tier_value_rank_2 as tier_value_rank_plus_tier_value_rank_2
    from valid_price_tiers
    where tier_value_rounded_rounded < 42
)
-- Multi-level aggregation
select
    case when tier_value >= 88 then 'PREMIUM' else 'STANDARD' end as tier_value_tier,
    max(tier_value_rounded) as max_tier_value_rounded,
    percentile_approx(tier_value_rounded_rounded, array(0.25, 0.5, 0.75)) as tier_value_rounded_rounded_quartiles,
    grouping(case when tier_value >= 88 then 'PREMIUM' else 'STANDARD' end) as gid
from cleaned
group by cube (case when tier_value >= 88 then 'PREMIUM' else 'STANDARD' end)
having max(tier_value_rounded) > 0
