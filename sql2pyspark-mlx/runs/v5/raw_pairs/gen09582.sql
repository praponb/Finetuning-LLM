with metrics as (
    select
        *,
        lag(revenue) over (partition by product_id, store_id order by units asc, id asc) prev_revenue,
        lag(units) over (partition by product_id, store_id order by units asc, id asc) prev_units,
        row_number() over (partition by product_id, store_id order by units asc, id asc) event_seq
    from store_sales
),
scored as (
    select
        *,
        lag(prev_units, 1) over (partition by store_id order by sale_date asc, id asc) prev_prev_units,
        cume_dist() over (order by prev_revenue asc nulls last) prev_revenue_cume_dist,
        ntile(10) over (order by prev_revenue asc, id asc) prev_revenue_bucket
    from metrics
    where revenue is null
),
latest as (
    select
        id,
        store_id,
        product_id,
        units,
        revenue,
        prev_revenue,
        prev_units,
        event_seq,
        prev_prev_units,
        prev_revenue_cume_dist,
        prev_revenue_bucket
    from scored
    qualify row_number() over (partition by product_id order by prev_prev_units desc nulls last, id asc) = 1
),
with_windows as (
    select
        id,
        product_id,
        units,
        revenue,
        prev_revenue_cume_dist,
        lead(prev_units, 1) over (partition by product_id, store_id order by revenue asc, id asc) next_prev_units,
        lag(prev_units, 1) over (order by prev_revenue_cume_dist asc, id asc) prev_prev_units_2
    from latest
),
grouped as (
    select
        case when units >= 32 then 'PREMIUM' else 'STANDARD' end as units_tier,
        approx_percentile(next_prev_units, array(0.25, 0.5, 0.75)) as next_prev_units_quartiles,
        sum(prev_prev_units_2) as total_prev_prev_units_2
    from with_windows
    group by 1
)
select units_tier, next_prev_units_quartiles
from grouped
order by units_tier desc
limit 20;
