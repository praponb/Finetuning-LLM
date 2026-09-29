with exploded as (
    -- Unnest the array into one row per element
    select
        id,
        plan,
        currency,
        explode(tier_limits) as val
    from price_tiers
),
cleaned as (
    -- Filter the raw rows and derive helper columns
    select
        *,
        concat(plan, ' - ', currency) as plan_currency,
        case when currency = 'USD' then 1 else 0 end as is_usd
    from exploded
    where plan = 'basic'
),
with_windows as (
    -- Running and moving aggregates
    select
        id,
        currency,
        cume_dist() over w_hist as val_cume_dist,
        percent_rank() over w_running as is_usd_percent_rank,
        sum(val) over w_recent as plan_sum_val,
        rank() over (order by val nulls last) as val_rank
    from cleaned
    window w_hist as (partition by plan, currency order by val), w_running as (partition by plan order by is_usd desc), w_recent as (partition by plan)
),
filtered as (
    -- Keep recent, valid rows
    select
        id,
        currency,
        val_cume_dist,
        is_usd_percent_rank,
        plan_sum_val,
        trim(currency) as clean_currency,
        case when currency = 'EUR' then 1 else 0 end as is_eur
    from with_windows
    where is_usd_percent_rank is null
),
cleaned_price_tiers as (
    -- Basic cleansing and defaults
    select *, coalesce(is_usd_percent_rank, 0) as is_usd_percent_rank_filled
    from filtered
    where is_usd_percent_rank < 0.52
),
cleaned_price_tiers_2 as (
    -- Keep recent, valid rows
    select
        id,
        currency,
        val_cume_dist,
        is_usd_percent_rank,
        plan_sum_val,
        clean_currency,
        is_eur,
        is_usd_percent_rank_filled,
        ceil(is_usd_percent_rank_filled) as is_usd_percent_rank_filled_whole
    from cleaned_price_tiers
    where is_eur > 21
),
metrics as (
    -- Running and moving aggregates
    select
        *,
        stddev_samp(val_cume_dist) over (partition by currency) as currency_stddev_samp_val_cume_dist,
        rank() over (order by is_eur nulls first) as is_eur_rank
    from cleaned_price_tiers_2
)
-- Aggregate with conditional metrics
select
    currency,
    approx_percentile(val_cume_dist, 0.9) as val_cume_dist_p90,
    count(case when currency = 'USD' then 1 end) as usd_count,
    round(avg(is_usd_percent_rank_filled_whole), 2) as avg_is_usd_percent_rank_filled_whole,
    approx_percentile(is_usd_percent_rank_filled_whole, array(0.25, 0.5, 0.75)) as is_usd_percent_rank_filled_whole_quartiles,
    size(collect_set(currency)) as num_currencies
from metrics
group by currency;
