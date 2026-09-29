with base as (
    -- Keep recent, valid rows
    select
        id,
        sale_date,
        units,
        revenue,
        coalesce(units, 0) as units_filled
    from store_sales
    where store_id in (1, 3, 5)
),
metrics as (
    -- Running and moving aggregates
    select
        id,
        sale_date,
        units,
        revenue,
        units_filled,
        sum(units_filled) over (order by units_filled, id rows between unbounded preceding and current row) as cumulative_units_filled,
        first_value(units, true) over (order by revenue, id rows between unbounded preceding and current row) as first_known_units
    from base
),
filtered as (
    -- Keep recent, valid rows
    select
        id,
        sale_date,
        units,
        revenue,
        cumulative_units_filled - revenue as cumulative_units_filled_minus_revenue
    from metrics
    where sale_date >= date '2023-11-12'
      and cumulative_units_filled < 351
),
with_windows as (
    -- Running and moving aggregates
    select
        *,
        ntile(3) over (order by cumulative_units_filled_minus_revenue, id) as cumulative_units_filled_minus_revenue_bucket,
        rank() over (order by revenue desc) as revenue_rank,
        sum(cumulative_units_filled_minus_revenue) over (order by cumulative_units_filled_minus_revenue, id rows between unbounded preceding and current row) as cumulative_cumulative_units_filled_minus_revenue,
        sum(cumulative_units_filled_minus_revenue) over (order by revenue, id rows between unbounded preceding and current row) as running_cumulative_units_filled_minus_revenue
    from filtered
),
report as (
    select
        id,
        sale_date,
        revenue,
        cumulative_units_filled_minus_revenue,
        cumulative_cumulative_units_filled_minus_revenue
    from with_windows
)
select
    id,
    revenue,
    cumulative_units_filled_minus_revenue,
    cumulative_cumulative_units_filled_minus_revenue
from report
