with base as (
    -- Keep recent, valid rows
    select
        *,
        units - revenue as revenue_per_unit,
        round(units, 2) as units_rounded
    from store_sales
    where store_id in (3, 4, 6)
),
enriched as (
    -- Attach products attributes
    select
        a.*,
        d.brand,
        d.price
    from base a
    left join products d
        on a.product_id = d.id
),
with_windows as (
    -- Running and moving aggregates
    select
        *,
        lag(units, 1) over (partition by brand order by sale_date, id) as prev_units,
        first_value(price) ignore nulls over (order by revenue, id rows between unbounded preceding and current row) as first_known_price,
        first_value(units_rounded) ignore nulls over (partition by brand, product_id order by price, id rows between unbounded preceding and current row) as first_known_units_rounded
    from enriched
),
enriched_store_sales as (
    -- Window metrics over several different partitions
    select
        *,
        sum(prev_units) over (partition by brand order by units, id rows between unbounded preceding and current row) as running_prev_units,
        sum(units) over (partition by store_id order by prev_units, id rows between unbounded preceding and current row) as cumulative_units,
        dense_rank() over (partition by store_id order by units_rounded desc nulls first) as units_rounded_dense_rank,
        percent_rank() over (partition by brand order by revenue_per_unit desc) as revenue_per_unit_percent_rank
    from with_windows
)
-- Running and moving aggregates
select
    id,
    store_id,
    product_id,
    sale_date,
    units,
    revenue,
    revenue_per_unit,
    units_rounded,
    brand,
    price,
    prev_units,
    first_known_price,
    first_known_units_rounded,
    running_prev_units,
    cumulative_units,
    units_rounded_dense_rank,
    revenue_per_unit_percent_rank,
    lag(revenue_per_unit_percent_rank, 2) over (partition by product_id order by price, id) as prev_revenue_per_unit_percent_rank,
    avg(first_known_price) over (partition by store_id, brand order by units_rounded, id rows between 3 preceding and current row) as first_known_price_avg_3
from enriched_store_sales;
