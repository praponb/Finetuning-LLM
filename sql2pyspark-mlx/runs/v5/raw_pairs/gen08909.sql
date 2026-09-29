with valid as (
    -- Keep recent, valid rows
    select
        id,
        product_id,
        quantity,
        last_restock,
        last_day(last_restock) as last_restock_month_end
    from inventory
    where quantity < 201
),
with_dims as (
    -- Attach products attributes
    select
        src.id,
        src.product_id,
        src.last_restock,
        b.stock_qty,
        b.brand
    from valid src
    inner join products b
        on src.product_id = b.id
    where b.brand = 'Stark'
),
enriched as (
    -- Attach products attributes
    select e.*, b.brand as product_brand
    from with_dims e
    inner join products b
        on e.product_id = b.id
),
ranked as (
    -- Keep one row per key (latest first)
    select *
    from (
        select *, rank() over (partition by product_id order by stock_qty desc) as rank_in_group
        from enriched
    ) ranked
    where rank_in_group = 1
),
metrics as (
    -- Running and moving aggregates
    select
        *,
        rank() over (order by stock_qty nulls last) as stock_qty_rank,
        max(stock_qty) over (partition by product_brand) as product_brand_max_stock_qty
    from ranked
),
filtered as (
    -- Basic cleansing and defaults
    select *, case when stock_qty >= 404 then 'High' when stock_qty >= 194 then 'Medium' else 'Low' end as stock_qty_band
    from metrics
    where brand is not null
),
grouped as (
    -- Grouped metrics
    select
        product_id,
        case when product_brand_max_stock_qty >= 339 then 'PREMIUM' else 'STANDARD' end as product_brand_max_stock_qty_tier,
        case when stock_qty_rank >= 8 then 'PREMIUM' else 'STANDARD' end as stock_qty_rank_tier,
        round(avg(product_brand_max_stock_qty), 2) as avg_product_brand_max_stock_qty,
        max(stock_qty) as max_stock_qty,
        round(avg(stock_qty_rank), 2) as avg_stock_qty_rank,
        grouping_id() as gid
    from filtered
    group by cube (product_id, case when product_brand_max_stock_qty >= 339 then 'PREMIUM' else 'STANDARD' end, case when stock_qty_rank >= 8 then 'PREMIUM' else 'STANDARD' end)
)
select
    product_id,
    product_brand_max_stock_qty_tier,
    avg_product_brand_max_stock_qty,
    max_stock_qty,
    avg_stock_qty_rank
from grouped
