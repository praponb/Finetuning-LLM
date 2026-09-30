with exploded as (
    -- Unnest the array into one row per element
    select
        id,
        effective_from,
        currency,
        explode_outer(discounts) as tier_value
    from price_tiers
),
with_windows as (
    -- Ranking and gap features
    select
        *,
        percent_rank() over (partition by currency order by tier_value desc nulls last) as tier_value_percent_rank,
        cume_dist() over (order by tier_value desc) as tier_value_cume_dist,
        avg(tier_value) over w_running as currency_avg_tier_value
    from exploded
    window w_running as (partition by currency)
),
metrics as (
    -- Window metrics over several different partitions
    select
        *,
        sum(currency_avg_tier_value) over (partition by currency) as currency_sum_currency_avg_tier_value,
        percent_rank() over w_grp as tier_value_percent_rank_2
    from with_windows
    window w_grp as (partition by currency order by tier_value desc nulls last)
),
metrics_price_tiers as (
    -- Running and moving aggregates
    select
        effective_from,
        currency,
        currency_sum_currency_avg_tier_value,
        tier_value_percent_rank_2,
        rank() over (partition by currency order by tier_value_percent_rank) as tier_value_percent_rank_rank,
        percent_rank() over (partition by currency order by currency_sum_currency_avg_tier_value desc nulls last) as currency_sum_currency_avg_tier_value_percent_rank,
        dense_rank() over (order by tier_value_cume_dist nulls last) as tier_value_cume_dist_dense_rank
    from metrics
)
select
    currency,
    tier_value_percent_rank_rank,
    tier_value_cume_dist_dense_rank
from metrics_price_tiers;
