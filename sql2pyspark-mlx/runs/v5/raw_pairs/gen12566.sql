select
    id,
    product_name,
    brand,
    price,
    cost,
    stock_qty,
    lag(stock_qty) over w_ordered as prev_stock_qty,
    lead(stock_qty) over w_ordered as next_stock_qty,
    lag(cost) over w_ordered as prev_cost,
    lead(cost) over w_ordered as next_cost,
    row_number() over w_ordered as row_seq
from products
window w_ordered as (partition by supplier_id order by price, id)
