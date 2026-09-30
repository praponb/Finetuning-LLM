with flattened as (
    -- Unnest the array into one row per element
    select
        id,
        plan,
        currency,
        tier_value
    from price_tiers
    lateral view outer explode(tier_limits) li as tier_value
),
metrics as (
    -- Running and moving aggregates
    select
        currency,
        tier_value,
        min(tier_value) over (partition by plan) as plan_min_tier_value,
        dense_rank() over (order by tier_value desc nulls first) as tier_value_dense_rank,
        sum(tier_value) over (partition by plan) as plan_sum_tier_value,
        avg(tier_value) over (partition by plan) as plan_avg_tier_value
    from flattened
    where tier_value >= 45
),
valid as (
    -- Keep recent, valid rows
    select *, substring(currency, 1, 4) as currency_prefix
    from metrics
    where currency not in ('EUR', 'THB')
),
cleaned as (
    -- Filter the raw rows and derive helper columns
    select
        tier_value,
        plan_min_tier_value,
        tier_value_dense_rank,
        plan_sum_tier_value,
        plan_avg_tier_value,
        currency_prefix,
        cast(plan_avg_tier_value as double) as plan_avg_tier_value_num,
        case when currency = 'USD' then 1 else 0 end as is_usd
    from valid
    where tier_value_dense_rank <= 4
)
-- Aggregate with conditional metrics
select
    case when tier_value >= 73 then 'HIGH' else 'LOW' end as tier_value_tier,
    percentile_approx(plan_sum_tier_value, 0.5) as plan_sum_tier_value_p50,
    count(*) as num_rows,
    count(*) as row_count,
    percentile_approx(tier_value, array(0.25, 0.5, 0.75)) as tier_value_quartiles
from cleaned
group by case when tier_value >= 73 then 'HIGH' else 'LOW' end;
