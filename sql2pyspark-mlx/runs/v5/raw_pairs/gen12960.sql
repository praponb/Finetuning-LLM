with valid as (
    select
        id,
        product_id,
        quantity,
        unit_price,
        greatest(unit_price, quantity) as greatest_unit_price_quantity,
        coalesce(quantity, 0) as quantity_filled
    from order_items
    where quantity between 5 and 7
),
with_windows as (
    select
        *,
        max(unit_price) over w_hist as last7_max_unit_price,
        min(unit_price) over w_hist as last7_min_unit_price,
        avg(unit_price) over w_hist as last7_avg_unit_price
    from valid
    where greatest_unit_price_quantity >= 78
    window w_hist as (partition by product_id order by quantity_filled, id rows between 6 preceding and current row)
),
with_windows_order_items as (
    select
        id,
        product_id,
        quantity,
        greatest_unit_price_quantity,
        ntile(10) over w_running as last7_min_unit_price_bucket,
        row_number() over (partition by product_id order by quantity desc, id) as row_seq,
        lag(unit_price, 2) over (order by last7_max_unit_price, id) as prev_unit_price,
        last_value(greatest_unit_price_quantity) ignore nulls over w_recent as last_known_greatest_unit_price_quantity,
        avg(quantity_filled) over (partition by product_id) as product_id_avg_quantity_filled
    from with_windows
    window w_running as (partition by product_id order by last7_min_unit_price desc, id), w_recent as (partition by product_id order by last7_avg_unit_price, id rows between unbounded preceding and current row)
),
with_dims as (
    select
        a.id,
        a.product_id,
        a.greatest_unit_price_quantity,
        a.row_seq,
        a.prev_unit_price,
        a.product_id_avg_quantity_filled,
        b.launch_date,
        b.stock_qty
    from with_windows_order_items a
    inner join products b
        on a.product_id = b.id
)
select
    id,
    greatest_unit_price_quantity,
    row_seq,
    product_id_avg_quantity_filled,
    launch_date,
    stock_qty
from with_dims
order by row_seq desc, stock_qty desc;
