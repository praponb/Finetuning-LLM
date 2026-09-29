with flattened as (
    -- Unnest the array into one row per element
    select
        id,
        currency,
        val
    from price_tiers
    lateral view explode(tier_limits) x as val
),
with_windows as (
    -- Window metrics over several different partitions
    select *, cume_dist() over (partition by currency order by val desc nulls first) as val_cume_dist
    from flattened
),
valid as (
    -- Basic cleansing and defaults
    select *, case when val_cume_dist >= 0.8 then 'High' when val_cume_dist >= 0.19 then 'Medium' else 'Low' end as val_cume_dist_level
    from with_windows
    where currency in ('THB', 'USD')
),
enriched as (
    -- Running and moving aggregates
    select
        *,
        stddev_samp(val) over w_running as currency_stddev_samp_val,
        min(val) over w_running as currency_min_val
    from valid
    window w_running as (partition by currency)
),
enriched_price_tiers as (
    -- Window metrics over several different partitions
    select
        *,
        count(val_cume_dist) over w_grp as currency_count_val_cume_dist,
        max(val_cume_dist) over w_grp as currency_max_val_cume_dist,
        min(currency_min_val) over w_grp as currency_min_currency_min_val,
        cume_dist() over (order by currency_stddev_samp_val asc nulls last) as currency_stddev_samp_val_cume_dist
    from enriched
    window w_grp as (partition by currency)
),
scored as (
    -- Running and moving aggregates
    select
        val_cume_dist_level,
        currency_stddev_samp_val,
        currency_min_val,
        currency_count_val_cume_dist,
        currency_max_val_cume_dist,
        currency_min_currency_min_val,
        dense_rank() over (partition by currency order by val_cume_dist desc nulls first) as val_cume_dist_dense_rank,
        cume_dist() over (partition by currency order by currency_min_currency_min_val desc nulls first) as currency_min_currency_min_val_cume_dist,
        avg(val_cume_dist) over w_running as currency_avg_val_cume_dist,
        avg(currency_stddev_samp_val) over w_running as currency_avg_currency_stddev_samp_val
    from enriched_price_tiers
    window w_running as (partition by currency)
),
filtered as (
    -- Keep recent, valid rows
    select
        *,
        currency_max_val_cume_dist + currency_min_currency_min_val_cume_dist as currency_max_val_cume_dist_plus_currency_min_currency_min_val_cume_dist,
        coalesce(currency_stddev_samp_val, 0) as currency_stddev_samp_val_filled
    from scored
    where currency_min_currency_min_val_cume_dist >= 0.29
)
-- Aggregate with conditional metrics
select
    case when currency_min_currency_min_val_cume_dist >= 0.77 then 'HIGH' else 'LOW' end as currency_min_currency_min_val_cume_dist_tier,
    case when currency_stddev_samp_val_filled >= 77 then 'PREMIUM' else 'STANDARD' end as currency_stddev_samp_val_filled_tier,
    percentile_approx(currency_avg_val_cume_dist, array(0.25, 0.5, 0.75)) as currency_avg_val_cume_dist_quartiles,
    approx_percentile(val_cume_dist_dense_rank, array(0.25, 0.5, 0.75)) as val_cume_dist_dense_rank_quartiles,
    avg(currency_max_val_cume_dist) as avg_currency_max_val_cume_dist,
    count(*) as num_rows
from filtered
group by rollup (case when currency_min_currency_min_val_cume_dist >= 0.77 then 'HIGH' else 'LOW' end, case when currency_stddev_samp_val_filled >= 77 then 'PREMIUM' else 'STANDARD' end)
having count(*) >= 1
