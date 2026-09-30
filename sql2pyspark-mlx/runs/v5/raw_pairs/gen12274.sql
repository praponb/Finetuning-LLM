select
    id,
    supplier_id,
    launch_date,
    stock_qty,
    stock_qty - lag(stock_qty, 1) over (partition by supplier_id order by launch_date, id) as stock_qty_change
from products
order by id desc, supplier_id desc;
