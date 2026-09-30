select
    *,
    ntile(3) over w_part as stock_qty_bucket,
    ntile(4) over w_hist as price_bucket
from products
window w_part as (partition by supplier_id order by stock_qty, id), w_hist as (partition by brand, supplier_id order by price desc, id)
