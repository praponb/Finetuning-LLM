select
    i.id,
    i.product_id,
    i.quantity,
    i.last_restock,
    ss.product_id as store_sale_product_id,
    ss.revenue,
    floor(i.quantity) as quantity_whole
from inventory i
join store_sales ss on i.store_id = ss.store_id and i.product_id = ss.product_id
order by revenue desc, i.id desc
