select
    id,
    category,
    launch_date,
    stock_qty,
    lag(stock_qty, 1) over (partition by category order by launch_date, id) as prev_stock_qty
from products;
