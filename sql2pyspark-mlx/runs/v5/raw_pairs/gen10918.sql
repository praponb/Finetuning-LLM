select
    i.id,
    i.product_id,
    i.store_id,
    i.last_restock,
    ss.sale_date,
    ss.revenue,
    ss.store_id store_sale_store_id
from inventory i
left join store_sales ss on i.store_id = ss.store_id and i.product_id = ss.product_id
