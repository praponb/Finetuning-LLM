with exploded as (
    -- Unnest the array into one row per element
    select
        id,
        plan,
        effective_from,
        currency,
        explode(discounts) as element
    from price_tiers
),
enriched as (
    -- Ranking and gap features
    select *, cume_dist() over (partition by currency order by element desc) element_cume_dist
    from exploded
),
metrics as (
    -- Window metrics over several different partitions
    select
        *,
        percent_rank() over (order by element_cume_dist nulls last) element_cume_dist_percent_rank,
        rank() over (partition by plan order by element desc nulls first) element_rank
    from enriched
),
filtered as (
    -- Basic cleansing and defaults
    select
        *,
        coalesce(element_cume_dist, 0) element_cume_dist_filled,
        date_trunc('YEAR', effective_from) effective_from_period
    from metrics
    where currency in ('USD', 'THB', 'EUR')
),
remaining_keys as (
    -- Set intersection on keys
    select plan, id
    from filtered
    intersect
    select plan, id
    from price_tiers
    where plan = 'basic'
),
kept_rows as (
    -- Keep rows whose keys survive the set operation
    select s.*
    from filtered s
    inner join remaining_keys k on s.plan = k.plan and s.id = k.id
),
scored as (
    -- Window metrics over several different partitions
    select
        plan,
        element,
        element_cume_dist_percent_rank,
        element_rank,
        element_cume_dist_filled,
        rank() over (partition by currency order by element_cume_dist nulls first) element_cume_dist_rank,
        rank() over (partition by plan order by element_cume_dist_percent_rank nulls first) element_cume_dist_percent_rank_rank,
        percent_rank() over w_hist element_cume_dist_filled_percent_rank,
        rank() over w_ordered element_rank_2
    from kept_rows
    where element_cume_dist > 0.79
    window w_hist as (order by element_cume_dist_filled desc), w_ordered as (partition by plan order by element desc nulls first)
),
metrics_price_tiers as (
    -- Running and moving aggregates
    select
        *,
        cume_dist() over (partition by plan order by element_rank_2 desc nulls last) element_rank_2_cume_dist,
        percent_rank() over (order by element_cume_dist_percent_rank nulls first) element_cume_dist_percent_rank_percent_rank,
        avg(element_cume_dist_filled) over w_running plan_avg_element_cume_dist_filled
    from scored
    window w_running as (partition by plan)
)
-- Native SQL UNPIVOT (wide to long)
select *
from (
    select
        plan,
        element_cume_dist_filled,
        element_cume_dist_percent_rank,
        element_rank_2_cume_dist
    from metrics_price_tiers
) src
unpivot (
    val for kpi in (element_cume_dist_filled as ELEMENT_CUME_DIST_FILLED, element_cume_dist_percent_rank as ELEMENT_CUME_DIST_PERCENT_RANK, element_rank_2_cume_dist as ELEMENT_RANK_2_CUME_DIST_VAL)
)
