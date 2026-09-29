with flattened as (
    -- Unnest the array into one row per element
    select
        id,
        effective_from,
        currency,
        explode_outer(tier_limits) as tier_value
    from price_tiers
),
cleaned as (
    -- Basic cleansing and defaults
    select *, upper(currency) as currency_upper
    from flattened
    where currency not in ('USD', 'EUR')
),
enriched as (
    -- Ranking and gap features
    select
        *,
        dense_rank() over w_running as tier_value_dense_rank,
        percent_rank() over w_running as tier_value_percent_rank
    from cleaned
    where effective_from < '2025-10-01'
    window w_running as (order by tier_value)
),
filtered as (
    -- Basic cleansing and defaults
    select *, month(effective_from) as effective_from_month
    from enriched
    where effective_from >= date '2024-05-12'
      and tier_value_dense_rank between 5 and 6
),
valid as (
    -- Keep recent, valid rows
    select *, month(effective_from) as effective_from_month_2
    from filtered
    where tier_value_dense_rank = 4
)
-- Window metrics over several different partitions
select
    id,
    currency_upper,
    tier_value_dense_rank,
    tier_value_percent_rank,
    effective_from_month_2,
    avg(tier_value_percent_rank) over w_hist as currency_avg_tier_value_percent_rank,
    count(tier_value_dense_rank) over w_hist as currency_count_tier_value_dense_rank,
    sum(tier_value_dense_rank) over w_hist as currency_sum_tier_value_dense_rank,
    max(effective_from_month_2) over w_hist as currency_max_effective_from_month_2,
    dense_rank() over (order by tier_value_percent_rank) as tier_value_percent_rank_dense_rank
from valid
window w_hist as (partition by currency)
