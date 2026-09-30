with flattened as (
    -- Unnest the array into one row per element
    select id, element
    from price_tiers
    lateral view explode(tier_limits) t as element
),
scored as (
    -- Ranking and gap features
    select
        id,
        element,
        percent_rank() over (order by element desc) as element_percent_rank,
        rank() over (order by element nulls first) as element_rank
    from flattened
    where element between 36 and 77
),
valid as (
    -- Basic cleansing and defaults
    select *, cast(element_rank as double) as element_rank_str
    from scored
    where element < 20
),
metrics as (
    -- Window metrics over several different partitions
    select
        element,
        element_percent_rank,
        rank() over (order by element) as element_rank_2,
        dense_rank() over w_ordered as element_rank_dense_rank,
        rank() over (order by element_rank desc nulls first) as element_rank_rank
    from valid
    window w_ordered as (order by element_rank desc nulls last)
),
cleaned as (
    -- Filter the raw rows and derive helper columns
    select
        element_percent_rank,
        element_rank_dense_rank,
        element_rank_rank,
        element + element_rank_dense_rank as element_plus_element_rank_dense_rank
    from metrics
    where element_percent_rank between 0.55 and 0.64
)
-- Aggregate with conditional metrics
select
    case when element_rank_rank >= 9 then 'LARGE' else 'SMALL' end as element_rank_rank_tier,
    case when element_plus_element_rank_dense_rank >= 90 then 'LARGE' else 'SMALL' end as element_plus_element_rank_dense_rank_tier,
    round(avg(element_rank_dense_rank), 2) as avg_element_rank_dense_rank,
    avg(element_rank_dense_rank) as avg_element_rank_dense_rank_2
from cleaned
group by case when element_rank_rank >= 9 then 'LARGE' else 'SMALL' end, case when element_plus_element_rank_dense_rank >= 90 then 'LARGE' else 'SMALL' end
order by avg_element_rank_dense_rank_2 nulls last
