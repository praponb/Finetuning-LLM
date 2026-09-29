select
    id,
    category,
    stock_qty,
    lag(price) over (partition by category order by price, id) prev_price,
    lead(price) over (partition by category order by price, id) next_price,
    lead(stock_qty) over (partition by category order by price, id) next_stock_qty,
    row_number() over (partition by category order by price, id) seq_no
from products
