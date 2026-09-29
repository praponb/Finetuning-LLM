with base as (
    -- Basic cleansing and defaults
    select *, round(price / cost, 2) as margin
    from products
    where month(launch_date) in (1, 3, 6)
),
with_windows as (
    -- Ranking and gap features
    select
        id,
        product_name,
        stock_qty,
        tags,
        supplier_id,
        margin,
        min(stock_qty) over (partition by brand order by price, id rows between unbounded preceding and current row) as running_min_stock_qty,
        count(stock_qty) over (partition by brand order by price, id rows between unbounded preceding and current row) as running_count_stock_qty,
        sum(stock_qty) over (partition by brand order by price, id rows between unbounded preceding and current row) as running_sum_stock_qty
    from base
),
with_windows_products as (
    -- Running and moving aggregates
    select
        *,
        last_value(margin, true) over (partition by supplier_id order by running_min_stock_qty, id rows between unbounded preceding and current row) as last_known_margin,
        ntile(10) over (partition by supplier_id order by running_sum_stock_qty, id) as running_sum_stock_qty_bucket,
        max(margin) over (partition by supplier_id) as supplier_id_max_margin
    from with_windows
),
latest as (
    -- Deduplicate with ROW_NUMBER
    select
        id,
        product_name,
        stock_qty,
        tags,
        supplier_id,
        margin,
        running_sum_stock_qty,
        running_sum_stock_qty_bucket,
        supplier_id_max_margin
    from (
        select *, row_number() over (partition by supplier_id order by running_count_stock_qty desc, id) as rank_in_group
        from with_windows_products
    ) t
    where rank_in_group = 1
)
-- Join a dimension and aggregate
select
    dim.city,
    dim.country,
    count(*) as rows_total,
    round(sum(src.stock_qty), 2) as total_stock_qty,
    round(avg(src.running_sum_stock_qty), 2) as avg_running_sum_stock_qty,
    grouping_id() as gid
from latest src
left join suppliers dim on src.supplier_id = dim.id
group by grouping sets ((dim.city, dim.country), (dim.city), ())
order by rows_total nulls last
