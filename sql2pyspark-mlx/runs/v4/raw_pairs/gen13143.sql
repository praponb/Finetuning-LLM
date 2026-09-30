with lines as (
    -- Unnest the array into one row per element
    select
        id,
        effective_from,
        currency,
        val
    from price_tiers
    lateral view explode(tier_prices) t as val
),
scored as (
    -- Running and moving aggregates
    select
        *,
        stddev_samp(val) over (partition by currency) as currency_stddev_samp_val,
        max(val) over (partition by currency) as currency_max_val,
        count(val) over (partition by currency) as currency_count_val,
        dense_rank() over (partition by currency order by val desc nulls first) as val_dense_rank
    from lines
),
metrics as (
    -- Window metrics over several different partitions
    select
        *,
        avg(currency_stddev_samp_val) over (partition by currency) as currency_avg_currency_stddev_samp_val,
        rank() over (partition by currency order by currency_count_val desc nulls first) as currency_count_val_rank,
        dense_rank() over (partition by currency order by currency_max_val desc nulls first) as currency_max_val_dense_rank
    from scored
),
metrics_price_tiers as (
    -- Ranking and gap features
    select
        *,
        percent_rank() over (partition by currency order by currency_max_val_dense_rank nulls first) as currency_max_val_dense_rank_percent_rank,
        stddev_samp(currency_max_val_dense_rank) over (partition by currency) as currency_stddev_samp_currency_max_val_dense_rank
    from metrics
),
with_windows as (
    -- Window metrics over several different partitions
    select
        *,
        dense_rank() over (partition by currency order by currency_stddev_samp_currency_max_val_dense_rank desc nulls first) as currency_stddev_samp_currency_max_val_dense_rank_dense_rank,
        sum(currency_avg_currency_stddev_samp_val) over w_running as currency_sum_currency_avg_currency_stddev_samp_val,
        percent_rank() over (order by val desc nulls last) as val_percent_rank
    from metrics_price_tiers
    window w_running as (partition by currency)
)
select
    id,
    effective_from,
    currency,
    val,
    currency_stddev_samp_val,
    currency_count_val,
    currency_avg_currency_stddev_samp_val,
    currency_count_val_rank,
    currency_max_val_dense_rank
from with_windows
order by currency_count_val nulls first
