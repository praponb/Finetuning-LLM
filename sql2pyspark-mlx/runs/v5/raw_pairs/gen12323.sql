select
    id,
    category,
    stock_qty,
    supplier_id,
    launch_date,
    percent_rank() over (partition by category order by stock_qty) as stock_qty_pct_rank,
    stock_qty - lag(stock_qty, 2) over (partition by supplier_id order by launch_date, id) as stock_qty_change
from products;
