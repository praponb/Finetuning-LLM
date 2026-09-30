with cleaned as (
    -- Keep recent, valid rows
    select
        id,
        store_id,
        product_id,
        sale_date,
        units,
        revenue,
        coalesce(revenue, 0) as revenue_filled
    from store_sales
    where sale_date > date '2023-06-30'
      and units > 28
),
latest as (
    -- Keep one row per key (latest first)
    select *
    from cleaned
    qualify row_number() over (partition by product_id order by revenue desc, id) = 1
),
with_windows as (
    -- Ranking and gap features
    select
        id,
        product_id,
        sale_date,
        revenue,
        cume_dist() over (partition by product_id, store_id order by revenue_filled nulls last) as revenue_filled_cume_dist,
        sum(revenue_filled) over (partition by product_id order by sale_date, id rows between unbounded preceding and current row) as running_revenue_filled
    from latest
    where sale_date <= '2024-12-01'
),
scored as (
    -- Ranking and gap features
    select
        *,
        max(revenue) over (partition by product_id order by running_revenue_filled, id rows between unbounded preceding and current row) as running_max_revenue,
        min(revenue) over (partition by product_id order by running_revenue_filled, id rows between unbounded preceding and current row) as running_min_revenue,
        sum(revenue) over (partition by product_id order by running_revenue_filled, id rows between unbounded preceding and current row) as running_sum_revenue,
        avg(revenue) over (partition by product_id order by running_revenue_filled, id rows between unbounded preceding and current row) as running_avg_revenue
    from with_windows
),
with_dims as (
    -- Attach products attributes
    select
        e.*,
        p.category,
        p.product_name,
        p.stock_qty
    from scored e
    left join products p
        on e.product_id = p.id
)
select
    id,
    product_id,
    revenue,
    revenue_filled_cume_dist,
    running_revenue_filled,
    running_max_revenue,
    running_min_revenue,
    running_sum_revenue,
    running_avg_revenue
from with_dims
