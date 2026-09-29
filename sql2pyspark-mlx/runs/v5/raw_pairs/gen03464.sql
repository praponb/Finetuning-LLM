with exploded as (
    -- Unnest the array into one row per element
    select
        id,
        currency,
        explode_outer(tier_prices) as element
    from price_tiers
),
enriched as (
    -- Window metrics over several different partitions
    select
        id,
        currency,
        element,
        cume_dist() over (partition by currency order by element desc) as element_cume_dist,
        cume_dist() over (partition by currency order by element nulls last) as element_cume_dist_2
    from exploded
),
valid as (
    -- Filter the raw rows and derive helper columns
    select
        id,
        currency,
        element,
        element_cume_dist,
        element_cume_dist_2,
        element * element_cume_dist as element_times_element_cume_dist
    from enriched
    where currency in ('THB', 'EUR')
),
base as (
    -- Keep recent, valid rows
    select
        id,
        element_cume_dist,
        element_cume_dist_2,
        element_times_element_cume_dist,
        round(element_cume_dist_2 * 1.07, 2) as element_cume_dist_2_scaled,
        cast(element_times_element_cume_dist as int) as element_times_element_cume_dist_int
    from valid
    where element is not null
),
metrics as (
    -- Ranking and gap features
    select
        element_times_element_cume_dist,
        element_cume_dist_2_scaled,
        element_times_element_cume_dist_int,
        percent_rank() over w_grp as element_cume_dist_2_scaled_percent_rank
    from base
    window w_grp as (order by element_cume_dist_2_scaled desc nulls first)
)
select element_cume_dist_2_scaled, element_times_element_cume_dist_int
from metrics
order by element_cume_dist_2_scaled desc nulls first;
