with exploded as (
    -- Unnest the array into one row per element
    select
        id,
        plan,
        currency,
        element
    from price_tiers
    lateral view outer explode(tier_prices) x as element
),
enriched as (
    -- Running and moving aggregates
    select
        *,
        cume_dist() over (order by element desc) as element_cume_dist,
        percent_rank() over w_hist as element_percent_rank
    from exploded
    window w_hist as (partition by plan order by element nulls last)
),
filtered as (
    -- Filter the raw rows and derive helper columns
    select
        plan,
        element,
        element_cume_dist,
        element_percent_rank + element as element_percent_rank_plus_element
    from enriched
    where element > 43
),
scored as (
    -- Window metrics over several different partitions
    select
        *,
        percent_rank() over w_recent as element_percent_rank,
        dense_rank() over w_recent as element_dense_rank,
        percent_rank() over (order by element nulls first) as element_percent_rank_2,
        dense_rank() over w_grp as element_cume_dist_dense_rank
    from filtered
    window w_recent as (partition by plan order by element desc nulls first), w_grp as (partition by plan order by element_cume_dist desc nulls first)
),
scored_price_tiers as (
    -- Running and moving aggregates
    select
        plan,
        element,
        element_cume_dist,
        element_percent_rank_plus_element,
        element_percent_rank,
        element_dense_rank,
        element_percent_rank_2,
        element_cume_dist_dense_rank,
        max(element_cume_dist) over w_hist as plan_max_element_cume_dist,
        max(element_cume_dist_dense_rank) over w_hist as plan_max_element_cume_dist_dense_rank
    from scored
    window w_hist as (partition by plan)
),
filtered_price_tiers as (
    -- Basic cleansing and defaults
    select
        plan,
        element,
        element_cume_dist,
        element_percent_rank_plus_element,
        element_percent_rank,
        element_dense_rank,
        element_percent_rank_2,
        plan_max_element_cume_dist,
        plan_max_element_cume_dist_dense_rank,
        coalesce(element_cume_dist, 0) as element_cume_dist_filled
    from scored_price_tiers
    where element_percent_rank <= 0.63
),
cleaned as (
    -- Keep recent, valid rows
    select
        *,
        coalesce(element_dense_rank, 0) as element_dense_rank_filled,
        element + element_cume_dist_filled as element_plus_element_cume_dist_filled
    from filtered_price_tiers
    where plan_max_element_cume_dist = 0
)
select
    plan,
    element_cume_dist,
    element_percent_rank_plus_element,
    element_percent_rank,
    element_dense_rank,
    element_percent_rank_2,
    plan_max_element_cume_dist,
    element_cume_dist_filled,
    element_dense_rank_filled
from cleaned
order by element_percent_rank nulls first
