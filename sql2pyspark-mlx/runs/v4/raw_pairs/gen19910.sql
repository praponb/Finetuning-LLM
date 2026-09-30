with filtered as (
    -- Basic cleansing and defaults
    select
        id,
        order_id,
        product_id,
        quantity,
        greatest(quantity, unit_price) as greatest_quantity_unit_price
    from order_items
    where unit_price < 260
),
latest as (
    -- Deduplicate with ROW_NUMBER
    select *
    from filtered
    qualify row_number() over (partition by order_id order by quantity desc, id) = 1
),
base as (
    -- Keep recent, valid rows
    select
        id,
        order_id,
        product_id,
        quantity,
        greatest_quantity_unit_price,
        quantity + greatest_quantity_unit_price as quantity_plus_greatest_quantity_unit_price,
        coalesce(quantity, 0) as quantity_filled
    from latest
    where order_id in (2, 18, 30)
),
enriched as (
    -- Attach products attributes
    select e.*, b.stock_qty
    from base e
    inner join products b
        on e.product_id = b.id
),
with_windows as (
    -- Running and moving aggregates
    select
        id,
        order_id,
        product_id,
        quantity,
        greatest_quantity_unit_price,
        quantity_plus_greatest_quantity_unit_price,
        quantity_filled,
        stock_qty,
        first_value(quantity_filled) ignore nulls over (partition by order_id order by stock_qty, id rows between unbounded preceding and current row) as first_known_quantity_filled,
        last_value(quantity, true) over (partition by product_id order by quantity_plus_greatest_quantity_unit_price, id rows between unbounded preceding and current row) as last_known_quantity,
        lead(greatest_quantity_unit_price, 1) over (partition by order_id order by stock_qty, id) as next_greatest_quantity_unit_price,
        ntile(5) over (partition by product_id order by quantity_plus_greatest_quantity_unit_price desc, id) as quantity_plus_greatest_quantity_unit_price_bucket,
        count(quantity) over (partition by order_id) as order_id_count_quantity
    from enriched
),
enriched_order_items as (
    -- Ranking and gap features
    select
        id,
        order_id,
        product_id,
        quantity,
        greatest_quantity_unit_price,
        quantity_plus_greatest_quantity_unit_price,
        quantity_filled,
        stock_qty,
        first_known_quantity_filled,
        last_known_quantity,
        next_greatest_quantity_unit_price,
        quantity_plus_greatest_quantity_unit_price_bucket,
        order_id_count_quantity,
        avg(last_known_quantity) over w_recent as last_known_quantity_avg_6,
        sum(stock_qty) over (partition by order_id, product_id order by quantity_filled, id rows between unbounded preceding and current row) as cumulative_stock_qty,
        ntile(5) over (partition by product_id, order_id order by first_known_quantity_filled desc, id) as first_known_quantity_filled_bucket
    from with_windows
    window w_recent as (partition by product_id order by next_greatest_quantity_unit_price, id rows between 6 preceding and current row)
)
select
    s.id,
    s.order_id,
    s.product_id,
    s.greatest_quantity_unit_price,
    s.stock_qty,
    s.first_known_quantity_filled,
    s.last_known_quantity,
    s.next_greatest_quantity_unit_price,
    s.order_id_count_quantity,
    (select min(x.units) from store_sales x where x.product_id = s.product_id) as min_store_sale_units
from enriched_order_items s
order by min_store_sale_units desc;
