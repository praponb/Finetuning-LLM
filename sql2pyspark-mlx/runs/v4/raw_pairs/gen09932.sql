with cleaned as (
    -- Basic cleansing and defaults
    select
        *,
        date_add(launch_date, 41) as launch_date_plus_41d,
        cast(cost as int) as cost_int,
        coalesce(brand, 'DIRECT') as brand_filled
    from products
    where launch_date > date '2023-06-01'
      and brand is not null
      and cost >= 720
),
matched_keys as (
    -- Set intersection on keys
    select category
    from cleaned
    intersect
    select category
    from products
    where cost <= 330
),
reconciled as (
    -- Keep rows whose keys survive the set operation
    select s.*
    from cleaned s
    inner join matched_keys k on s.category = k.category
),
scored as (
    -- Running and moving aggregates
    select
        *,
        cume_dist() over (order by stock_qty desc nulls first) as stock_qty_cume_dist,
        rank() over (partition by supplier_id order by stock_qty desc) as stock_qty_rank,
        rank() over (partition by category order by stock_qty desc nulls last) as stock_qty_rank_2
    from reconciled
),
valid as (
    -- Keep recent, valid rows
    select *, concat_ws(',', brand, product_name) as brand_product_name_key
    from scored
    where launch_date > to_date('2023-03-22')
      and brand is not null
)
select
    id,
    cost,
    stock_qty,
    tags,
    supplier_id,
    launch_date_plus_41d,
    stock_qty_cume_dist,
    stock_qty_rank_2,
    brand_product_name_key
from valid
order by stock_qty desc, stock_qty_rank_2 desc;
