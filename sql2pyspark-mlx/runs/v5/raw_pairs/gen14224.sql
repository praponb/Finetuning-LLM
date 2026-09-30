with exploded as (
    -- Unnest the array into one row per element
    select
        id,
        effective_from,
        element
    from price_tiers
    lateral view explode(tier_prices) t as element
),
with_windows as (
    -- Window metrics over several different partitions
    select *, percent_rank() over (order by element desc nulls last) as element_percent_rank
    from exploded
),
with_windows_price_tiers as (
    -- Running and moving aggregates
    select *, rank() over (order by element_percent_rank desc) as element_percent_rank_rank
    from with_windows
    where element <= 70
),
filtered as (
    -- Keep recent, valid rows
    select *, date_trunc('month', effective_from) as effective_from_period
    from with_windows_price_tiers
    where effective_from > to_date('2024-04-19')
),
scored as (
    -- Window metrics over several different partitions
    select
        id,
        element,
        element_percent_rank,
        element_percent_rank_rank,
        count(element) over (order by cast(effective_from_period as bigint) asc range between 604800 preceding and current row) as element_count_7d,
        percent_rank() over (order by element desc) as element_percent_rank_2
    from filtered
),
metrics as (
    -- Window metrics over several different partitions
    select *, cume_dist() over (order by element_percent_rank_2 desc) as element_percent_rank_2_cume_dist
    from scored
)
select
    id,
    element,
    element_percent_rank_rank,
    element_count_7d,
    element_percent_rank_2,
    element_percent_rank_2_cume_dist
from metrics
order by element_count_7d asc;
