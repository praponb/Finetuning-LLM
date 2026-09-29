select
    id,
    supplier_id,
    stock_qty,
    row_number() over (partition by supplier_id order by stock_qty desc, id) as stock_qty_row
from products
where supplier_id = 20
