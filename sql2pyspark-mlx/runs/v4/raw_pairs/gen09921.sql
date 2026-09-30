with exploded as (
    -- Unnest the array into one row per element
    select
        id,
        plan,
        element
    from price_tiers
    lateral view explode(discounts) e as element
),
cleaned as (
    -- Keep recent, valid rows
    select *, length(plan) as plan_length
    from exploded
    where element >= 56
),
with_windows as (
    -- Running and moving aggregates
    select
        *,
        dense_rank() over (partition by plan order by element desc nulls first) as element_dense_rank,
        rank() over (partition by plan order by element) as element_rank,
        dense_rank() over (partition by plan order by plan_length desc nulls last) as plan_length_dense_rank,
        cume_dist() over (order by element) as element_cume_dist
    from cleaned
),
cleaned_price_tiers as (
    -- Keep recent, valid rows
    select
        element,
        element_dense_rank,
        element_rank,
        plan_length_dense_rank,
        element_cume_dist,
        cast(element_dense_rank as double) as element_dense_rank_num
    from with_windows
    where element_dense_rank > 8
),
filtered as (
    -- Basic cleansing and defaults
    select
        element,
        element_dense_rank,
        element_rank,
        plan_length_dense_rank,
        element_cume_dist,
        floor(element_dense_rank_num) as element_dense_rank_num_whole
    from cleaned_price_tiers
    where element_dense_rank_num >= 64
),
base as (
    -- Basic cleansing and defaults
    select *, case when element >= 67 then 'High' when element >= 30 then 'Medium' else 'Low' end as element_band
    from filtered
    where element <= 80
)
-- Grouped metrics
select
    case when element >= 61 then 'LARGE' else 'SMALL' end as element_tier,
    case when element_dense_rank >= 8 then 'PREMIUM' else 'STANDARD' end as element_dense_rank_tier,
    percentile_approx(element_dense_rank, array(0.25, 0.5, 0.75)) as element_dense_rank_quartiles,
    approx_percentile(element_rank, array(0.25, 0.5, 0.75)) as element_rank_quartiles,
    grouping(case when element >= 61 then 'LARGE' else 'SMALL' end) as gid
from base
group by grouping sets ((case when element >= 61 then 'LARGE' else 'SMALL' end, case when element_dense_rank >= 8 then 'PREMIUM' else 'STANDARD' end), (case when element >= 61 then 'LARGE' else 'SMALL' end), ());
