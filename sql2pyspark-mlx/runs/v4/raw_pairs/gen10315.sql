with exploded as (
    -- Unnest the array into one row per element
    select
        id,
        currency,
        explode_outer(tier_prices) as element
    from price_tiers
),
metrics as (
    -- Window metrics over several different partitions
    select
        *,
        dense_rank() over (partition by currency order by element desc nulls first) as element_dense_rank,
        max(element) over (partition by currency) as currency_max_element
    from exploded
),
enriched as (
    -- Window metrics over several different partitions
    select
        *,
        stddev_samp(element_dense_rank) over w_part as currency_stddev_samp_element_dense_rank,
        cume_dist() over w_grp as element_dense_rank_cume_dist,
        max(currency_max_element) over w_part as currency_max_currency_max_element,
        count(element_dense_rank) over w_part as currency_count_element_dense_rank,
        cume_dist() over (partition by currency order by currency_max_element desc nulls last) as currency_max_element_cume_dist
    from metrics
    window w_part as (partition by currency), w_grp as (partition by currency order by element_dense_rank desc nulls first)
),
cleaned as (
    -- Filter the raw rows and derive helper columns
    select *, element * currency_stddev_samp_element_dense_rank as element_times_currency_stddev_samp_element_dense_rank
    from enriched
    where currency_max_element is not null
),
filtered as (
    -- Filter the raw rows and derive helper columns
    select
        *,
        round(element_dense_rank_cume_dist * 100, 2) as element_dense_rank_cume_dist_adjusted,
        greatest(currency_count_element_dense_rank, currency_max_currency_max_element) as greatest_currency_count_element_dense_rank_currency_max_currency_max_element
    from cleaned
    where element is not null
),
enriched_price_tiers as (
    -- Ranking and gap features
    select
        *,
        avg(currency_max_currency_max_element) over w_part as currency_avg_currency_max_currency_max_element,
        percent_rank() over w_grp as currency_stddev_samp_element_dense_rank_percent_rank,
        min(currency_stddev_samp_element_dense_rank) over w_part as currency_min_currency_stddev_samp_element_dense_rank
    from filtered
    window w_part as (partition by currency), w_grp as (order by currency_stddev_samp_element_dense_rank desc nulls first)
),
grouped as (
    -- Grouped metrics
    select
        case when element_dense_rank >= 7 then 'LARGE' else 'SMALL' end as element_dense_rank_tier,
        sum(currency_avg_currency_max_currency_max_element) as total_currency_avg_currency_max_currency_max_element,
        percentile_approx(element, 0.9) as element_p90,
        count(*) as num_rows
    from enriched_price_tiers
    group by 1
)
select
    element_dense_rank_tier,
    total_currency_avg_currency_max_currency_max_element,
    num_rows
from grouped
order by num_rows desc nulls first;
