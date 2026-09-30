with flattened as (
    -- Unnest the array into one row per element
    select id, explode(tier_prices) as val
    from price_tiers
),
filtered as (
    -- Basic cleansing and defaults
    select
        *,
        coalesce(val, 0) as val_filled,
        round(val * 0.9, 2) as adjusted_val
    from flattened
    where val between 20 and 32
),
kept_keys as (
    -- Set difference on keys
    select id
    from filtered
    except
    select id
    from price_tiers
    where effective_from >= '2025-09-28'
),
reconciled as (
    -- Keep rows whose keys survive the set operation
    select s.*
    from filtered s
    inner join kept_keys k on s.id = k.id
),
cleaned as (
    -- Keep recent, valid rows
    select *, least(val, adjusted_val) as least_val_adjusted_val
    from reconciled
    where adjusted_val between 28 and 54
)
-- Aggregate with conditional metrics
select
    case when least_val_adjusted_val >= 64 then 'HIGH' else 'LOW' end as least_val_adjusted_val_tier,
    case when val_filled >= 69 then 'LARGE' else 'SMALL' end as val_filled_tier,
    count(distinct id) as distinct_ids,
    count(*) as cnt,
    round(avg(val_filled), 2) as avg_val_filled
from cleaned
group by case when least_val_adjusted_val >= 64 then 'HIGH' else 'LOW' end, case when val_filled >= 69 then 'LARGE' else 'SMALL' end
order by distinct_ids desc;
