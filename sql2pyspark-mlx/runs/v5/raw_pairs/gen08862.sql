with with_windows as (
    -- Ranking and gap features
    select
        *,
        min(stock_qty) over (partition by category, supplier_id order by launch_date, id rows between 3 preceding and current row) as last4_min_stock_qty,
        max(stock_qty) over (partition by category, supplier_id order by launch_date, id rows between 3 preceding and current row) as last4_max_stock_qty,
        avg(stock_qty) over (partition by category, supplier_id order by launch_date, id rows between 3 preceding and current row) as last4_avg_stock_qty
    from products
    where launch_date <= '2024-10-26'
),
not_flagged as (
    -- Anti-join via a correlated subquery
    select s.*
    from with_windows s
    where not exists (
            select 1
            from order_items x
            where x.product_id = s.id
              and x.quantity between 4 and 7
        )
),
matched_keys as (
    -- Set intersection on keys
    select brand
    from not_flagged
    intersect
    select brand
    from products
    where launch_date <= '2024-09-01'
),
reconciled as (
    -- Keep rows whose keys survive the set operation
    select s.*
    from not_flagged s
    inner join matched_keys k on s.brand = k.brand
),
with_windows_products as (
    -- Window metrics over several different partitions
    select
        *,
        cume_dist() over (order by cost desc) as cost_cume_dist,
        percent_rank() over (partition by supplier_id, category order by last4_max_stock_qty desc nulls first) as last4_max_stock_qty_percent_rank,
        rank() over (partition by brand order by cost desc nulls first) as cost_rank,
        stddev_samp(price) over (partition by category) as category_stddev_samp_price,
        count(last4_min_stock_qty) over (partition by category) as category_count_last4_min_stock_qty
    from reconciled
),
scored as (
    -- Running and moving aggregates
    select
        *,
        cume_dist() over (partition by category order by stock_qty desc) as stock_qty_cume_dist,
        corr(price, stock_qty) over (partition by supplier_id) as corr_price_stock_qty
    from with_windows_products
)
-- Window metrics over several different partitions
select
    id,
    product_name,
    category,
    brand,
    price,
    cost,
    stock_qty,
    launch_date,
    supplier_id,
    last4_min_stock_qty,
    last4_max_stock_qty,
    last4_max_stock_qty_percent_rank,
    category_stddev_samp_price,
    category_count_last4_min_stock_qty,
    stock_qty_cume_dist,
    round(category_stddev_samp_price / sum(category_stddev_samp_price) over (partition by category), 4) as category_stddev_samp_price_share
from scored;
