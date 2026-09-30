with valid as (
    -- Basic cleansing and defaults
    select
        id,
        store_id,
        product_id,
        sale_date,
        units,
        round(revenue / units, 2) as revenue_per_unit
    from store_sales
    where sale_date >= to_date('2023-07-10')
)
-- Ranking and gap features
select
    *,
    round(units / sum(units) over (partition by store_id, product_id), 4) as units_share,
    sum(revenue_per_unit) over (partition by product_id, store_id order by revenue_per_unit, id rows between unbounded preceding and current row) as cumulative_revenue_per_unit,
    percent_rank() over (partition by store_id, product_id order by revenue_per_unit nulls last) as revenue_per_unit_percent_rank,
    lag(units, 1) over (order by units, id) as prev_units
from valid
