with lines as (
    -- Unnest the array into one row per element
    select
        id,
        currency,
        explode_outer(tier_prices) as tier_value
    from price_tiers
),
metrics as (
    -- Ranking and gap features
    select *, min(tier_value) over w_recent as currency_min_tier_value
    from lines
    window w_recent as (partition by currency)
),
scored as (
    -- Window metrics over several different partitions
    select
        id,
        currency_min_tier_value,
        stddev_samp(tier_value) over w_part as currency_stddev_samp_tier_value,
        max(currency_min_tier_value) over w_part as currency_max_currency_min_tier_value
    from metrics
    window w_part as (partition by currency)
),
metrics_price_tiers as (
    -- Running and moving aggregates
    select *, rank() over (order by currency_stddev_samp_tier_value nulls first) as currency_stddev_samp_tier_value_rank
    from scored
),
filtered as (
    -- Filter the raw rows and derive helper columns
    select
        *,
        currency_stddev_samp_tier_value_rank * currency_min_tier_value as currency_stddev_samp_tier_value_rank_times_currency_min_tier_value,
        currency_min_tier_value / nullif(currency_stddev_samp_tier_value, 0) as currency_min_tier_value_per_currency_stddev_samp_tier_value
    from metrics_price_tiers
    where currency_stddev_samp_tier_value > 69
),
with_windows as (
    -- Running and moving aggregates
    select
        currency_min_tier_value,
        currency_stddev_samp_tier_value,
        currency_stddev_samp_tier_value_rank,
        cume_dist() over (order by currency_max_currency_min_tier_value nulls first) as currency_max_currency_min_tier_value_cume_dist,
        dense_rank() over (order by currency_min_tier_value_per_currency_stddev_samp_tier_value) as currency_min_tier_value_per_currency_stddev_samp_tier_value_dense_rank
    from filtered
)
-- Aggregate with conditional metrics
select
    case when currency_stddev_samp_tier_value >= 88 then 'HIGH' else 'LOW' end as currency_stddev_samp_tier_value_tier,
    approx_percentile(currency_stddev_samp_tier_value, array(0.25, 0.5, 0.75)) as currency_stddev_samp_tier_value_quartiles,
    count(*) as row_count,
    round(avg(currency_stddev_samp_tier_value), 2) as avg_currency_stddev_samp_tier_value,
    percentile_approx(currency_stddev_samp_tier_value_rank, array(0.25, 0.5, 0.75)) as currency_stddev_samp_tier_value_rank_quartiles
from with_windows
group by 1
order by avg_currency_stddev_samp_tier_value nulls first;
