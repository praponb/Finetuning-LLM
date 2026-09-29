with base as (
    select
        id,
        category,
        brand,
        cost,
        stock_qty,
        launch_date,
        tags,
        supplier_id,
        stock_qty + price stock_qty_plus_price,
        coalesce(brand, 'UNKNOWN') brand_clean
    from products
    where brand not in ('Globex', 'Umbrella', 'Acme')
),
with_latest as (
    select
        s.id,
        s.category,
        s.stock_qty,
        s.launch_date,
        s.tags,
        s.stock_qty_plus_price,
        s.brand_clean,
        x.order_item_unit_price
    from base s
    join lateral (
            select c.unit_price as order_item_unit_price
            from order_items c
            where c.product_id = s.id
            order by c.unit_price desc, c.id desc
            limit 1
        ) x on true
),
metrics as (
    select
        *,
        lead(order_item_unit_price) over (partition by category order by stock_qty, id) next_order_item_unit_price,
        lag(order_item_unit_price) over (partition by category order by stock_qty, id) prev_order_item_unit_price,
        lead(stock_qty_plus_price) over (partition by category order by stock_qty, id) next_stock_qty_plus_price,
        lag(stock_qty_plus_price) over (partition by category order by stock_qty, id) prev_stock_qty_plus_price,
        row_number() over (partition by category order by stock_qty, id) row_seq,
        ntile(3) over (partition by category order by stock_qty, id) stock_qty_bucket
    from with_latest
),
scored as (
    select
        *,
        avg(stock_qty) over (partition by category order by row_seq, id rows between 2 preceding and 1 preceding) stock_qty_avg_2_prior,
        lag(next_order_item_unit_price, 2) over (partition by category order by order_item_unit_price, id) prev_next_order_item_unit_price,
        row_number() over (partition by category order by next_stock_qty_plus_price desc, id) row_seq_2,
        avg(next_stock_qty_plus_price) over (partition by category order by prev_stock_qty_plus_price, id rows between 2 preceding and 1 preceding) next_stock_qty_plus_price_avg_2_prior,
        last_value(row_seq) ignore nulls over (order by next_order_item_unit_price, id rows between unbounded preceding and current row) last_known_row_seq
    from metrics
),
with_last as (
    select
        s.id,
        s.category,
        s.stock_qty,
        s.launch_date,
        s.tags,
        s.stock_qty_plus_price,
        s.next_order_item_unit_price,
        s.next_stock_qty_plus_price,
        s.stock_qty_bucket,
        s.stock_qty_avg_2_prior,
        s.row_seq_2,
        s.next_stock_qty_plus_price_avg_2_prior,
        latest.last_restock
    from scored s
    left join lateral (
            select c.last_restock
            from inventory c
            where c.product_id = s.id
              and c.last_restock <= s.launch_date
            order by c.last_restock desc, c.id desc
            limit 1
        ) latest on true
),
report as (
    select
        id,
        category,
        stock_qty,
        launch_date,
        tags,
        next_order_item_unit_price,
        next_stock_qty_plus_price,
        stock_qty_bucket,
        stock_qty_avg_2_prior
    from with_last
    order by next_order_item_unit_price desc, next_stock_qty_plus_price desc
)
select
    category,
    stock_qty,
    next_order_item_unit_price,
    next_stock_qty_plus_price,
    stock_qty_bucket,
    stock_qty_avg_2_prior
from report
order by next_order_item_unit_price desc
