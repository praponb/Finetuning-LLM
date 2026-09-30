select
    i.id,
    i.store_id,
    i.last_restock,
    i.product_id,
    ss.sale_date,
    ss.product_id as store_sale_product_id
from inventory i
join store_sales ss on i.store_id = ss.store_id and i.product_id = ss.product_id
order by id desc
