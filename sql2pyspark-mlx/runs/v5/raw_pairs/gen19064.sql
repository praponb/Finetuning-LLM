with exploded as (
    -- Unnest the array into one row per element
    select
        id,
        effective_from,
        currency,
        element
    from price_tiers
    lateral view explode(discounts) x as element
),
valid as (
    -- Keep recent, valid rows
    select
        *,
        coalesce(element, 0) element_filled,
        case when element >= 69 then 'High' when element >= 26 then 'Medium' else 'Low' end element_tier
    from exploded
    where effective_from >= to_date('2024-06-20')
),
with_windows as (
    -- Window metrics over several different partitions
    select
        id,
        element_filled,
        element_tier,
        cume_dist() over (order by element desc nulls first) element_cume_dist,
        max(element) over (partition by currency) currency_max_element,
        percent_rank() over (order by element_filled) element_filled_percent_rank,
        dense_rank() over (partition by currency order by element_filled nulls last) element_filled_dense_rank
    from valid
    where year(effective_from) = 2025
),
filtered as (
    -- Keep recent, valid rows
    select *, element_cume_dist / nullif(element_filled, 0) element_cume_dist_per_element_filled
    from with_windows
    where currency_max_element > 42
),
summary as (
    -- Multi-level aggregation
    select
        case when element_filled >= 83 then 'PREMIUM' else 'STANDARD' end as element_filled_tier,
        case when element_filled_dense_rank >= 8 then 'HIGH' else 'LOW' end as element_filled_dense_rank_tier,
        percentile_approx(element_filled_percent_rank, array(0.25, 0.5, 0.75)) as element_filled_percent_rank_quartiles,
        sum(element_filled_percent_rank) as total_element_filled_percent_rank,
        approx_percentile(element_filled, array(0.25, 0.5, 0.75)) as element_filled_quartiles,
        sum(element_cume_dist_per_element_filled) as total_element_cume_dist_per_element_filled,
        count(*) as event_count
    from filtered
    group by rollup (case when element_filled >= 83 then 'PREMIUM' else 'STANDARD' end, case when element_filled_dense_rank >= 8 then 'HIGH' else 'LOW' end)
)
select
    element_filled_tier,
    element_filled_dense_rank_tier,
    element_filled_percent_rank_quartiles,
    total_element_cume_dist_per_element_filled,
    event_count
from summary
