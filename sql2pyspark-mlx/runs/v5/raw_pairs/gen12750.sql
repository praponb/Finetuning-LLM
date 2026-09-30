with exploded as (
    -- Unnest the array into one row per element
    select id, val
    from price_tiers
    lateral view outer explode(discounts) li as val
)
-- Multi-level aggregation
select
    case when val >= 79 then 'HIGH' else 'LOW' end as val_tier,
    min(val) as min_val,
    min(val) as min_val_2,
    sum(val) as total_val,
    percentile_approx(val, 0.5) as val_p50,
    count(distinct id) as distinct_ids,
    approx_percentile(val, 0.95) as val_p95
from exploded
group by case when val >= 79 then 'HIGH' else 'LOW' end
order by min_val desc;
