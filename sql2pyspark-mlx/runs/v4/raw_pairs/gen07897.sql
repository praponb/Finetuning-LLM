with latest as (
    -- Deduplicate with ROW_NUMBER
    select *
    from products
    qualify row_number() over (partition by supplier_id order by launch_date desc, id) = 1
),
enriched as (
    -- Attach suppliers attributes
    select
        src.*,
        b.rating,
        b.country,
        b.city
    from latest src
    left join suppliers b
        on src.supplier_id = b.id
),
metrics as (
    -- Ranking and gap features
    select
        *,
        sum(rating) over (partition by category order by price, id rows between unbounded preceding and current row) as cumulative_rating,
        ntile(3) over (order by stock_qty, id) as stock_qty_bucket
    from enriched
),
scored as (
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
        tags,
        supplier_id,
        rating,
        country,
        city,
        cumulative_rating,
        stock_qty_bucket,
        dense_rank() over (partition by brand order by stock_qty desc) as stock_qty_dense_rank,
        avg(price) over (partition by category) as category_avg_price,
        sum(stock_qty) over (partition by brand) as brand_sum_stock_qty
    from metrics
),
valid as (
    -- Filter the raw rows and derive helper columns
    select
        id,
        product_name,
        category,
        brand,
        price,
        cost,
        stock_qty,
        tags,
        city,
        cumulative_rating,
        stock_qty_bucket,
        stock_qty_dense_rank,
        category_avg_price,
        greatest(cost, brand_sum_stock_qty) as greatest_cost_brand_sum_stock_qty
    from scored
    where category != 'Sports'
),
base as (
    -- Basic cleansing and defaults
    select *, concat_ws(',', category, city) as category_city_key
    from valid
    where brand is not null
),
enriched_products as (
    -- Running and moving aggregates
    select
        *,
        sum(stock_qty_dense_rank) over (partition by category order by cost, id rows between unbounded preceding and current row) as cumulative_stock_qty_dense_rank,
        avg(stock_qty_dense_rank) over (order by stock_qty_bucket, id rows between 6 preceding and current row) as stock_qty_dense_rank_avg_6,
        ntile(3) over w_part as cumulative_rating_bucket
    from base
    window w_part as (partition by city order by cumulative_rating desc, id)
)
-- Native SQL PIVOT
select *
from (
    select
        city,
        category,
        stock_qty_dense_rank_avg_6
    from enriched_products
) src
pivot (
    sum(stock_qty_dense_rank_avg_6) as amount
    for category in ('Electronics', 'Books')
)
