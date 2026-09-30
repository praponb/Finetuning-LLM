with exploded as (
    -- Unnest the array into one row per element
    select
        id,
        currency,
        explode_outer(tier_prices) as val
    from price_tiers
),
base as (
    -- Filter the raw rows and derive helper columns
    select
        *,
        length(currency) as currency_length,
        coalesce(val, 0) as val_filled
    from exploded
    where val > 47
),
enriched as (
    -- Ranking and gap features
    select
        *,
        dense_rank() over (order by currency_length nulls first) as currency_length_dense_rank,
        percent_rank() over (partition by currency order by val nulls first) as val_percent_rank
    from base
),
remaining_keys as (
    -- Set intersection on keys
    select id, currency
    from enriched
    intersect all
    select id, currency
    from price_tiers
    where effective_from > '2024-04-01'
),
kept_rows as (
    -- Keep rows whose keys survive the set operation
    select s.*
    from enriched s
    inner join remaining_keys k on s.id = k.id and s.currency = k.currency
),
with_windows as (
    -- Window metrics over several different partitions
    select
        id,
        currency,
        val,
        val_filled,
        val_percent_rank,
        stddev_samp(currency_length_dense_rank) over (partition by currency) as currency_stddev_samp_currency_length_dense_rank
    from kept_rows
)
-- Multi-level aggregation
select
    currency,
    avg(val_percent_rank) as avg_val_percent_rank,
    size(collect_set(currency)) as num_currencies,
    count(case when currency = 'EUR' then 1 end) as eur_count,
    percentile_approx(val, 0.9) as val_p90,
    count(case when currency = 'THB' then 1 end) as thb_count,
    sum(case when currency = 'EUR' then val_percent_rank else 0 end) as eur_val_percent_rank
from with_windows
group by currency
