with cleaned as (
    -- Keep recent, valid rows
    select
        id,
        product_name,
        category,
        brand,
        price,
        stock_qty,
        tags,
        supplier_id,
        coalesce(stock_qty, 0) as stock_qty_filled,
        coalesce(brand, 'UNKNOWN') as brand_clean
    from products
    where stock_qty is not null
      and launch_date >= date '2023-05-13'
),
metrics as (
    -- Running and moving aggregates
    select
        id,
        product_name,
        brand,
        stock_qty,
        tags,
        brand_clean,
        avg(stock_qty_filled) over (order by price, id rows between 6 preceding and current row) as stock_qty_filled_avg_6,
        avg(stock_qty) over w_recent as stock_qty_avg_2,
        row_number() over (partition by brand order by price desc, id) as row_seq,
        ntile(5) over (order by stock_qty_filled desc, id) as stock_qty_filled_bucket
    from cleaned
    window w_recent as (partition by category order by stock_qty, id rows between 2 preceding and current row)
),
matched_keys as (
    -- Set intersection on keys
    select id, brand
    from metrics
    intersect all
    select id, brand
    from products
    where stock_qty != 112
),
verified as (
    -- Keep rows whose keys survive the set operation
    select s.*
    from metrics s
    inner join matched_keys k on s.id = k.id and s.brand = k.brand
),
valid as (
    -- Keep recent, valid rows
    select
        id,
        product_name,
        brand,
        tags,
        brand_clean,
        stock_qty_filled_avg_6,
        stock_qty_avg_2,
        stock_qty_filled_bucket,
        initcap(trim(brand)) as brand_title,
        upper(brand) as brand_upper,
        coalesce(brand, 'N/A') as resolved_brand
    from verified
    where brand is not null
      and stock_qty between 304 and 388
),
cleaned_products as (
    -- Keep recent, valid rows
    select *, stock_qty_filled_bucket - stock_qty_filled_avg_6 as stock_qty_filled_bucket_minus_stock_qty_filled_avg_6
    from valid
    where brand is not null
      and stock_qty_filled_bucket != 3
),
scored as (
    -- Window metrics over several different partitions
    select
        id,
        product_name,
        brand,
        tags,
        brand_clean,
        brand_title,
        resolved_brand,
        percent_rank() over (partition by brand order by stock_qty_avg_2 nulls first) as stock_qty_avg_2_percent_rank,
        dense_rank() over (partition by brand order by stock_qty_filled_bucket_minus_stock_qty_filled_avg_6 desc nulls last) as stock_qty_filled_bucket_minus_stock_qty_filled_avg_6_dense_rank
    from cleaned_products
    where stock_qty_filled_bucket_minus_stock_qty_filled_avg_6 between 21 and 52
),
eligible as (
    -- Anti-join via a correlated subquery
    select s.*
    from scored s
    where not exists (
            select 1
            from order_items ch
            where ch.product_id = s.id
              and ch.order_id in (14, 18, 38)
        )
)
select
    id,
    product_name,
    tags,
    brand_clean,
    brand_title,
    resolved_brand,
    stock_qty_avg_2_percent_rank,
    case when product_name rlike '^(USB-C Hub|Smart Watch)$' then 'PRIORITY' else 'GROUP_A' end as product_name_zone
from eligible
order by stock_qty_avg_2_percent_rank desc nulls last;
