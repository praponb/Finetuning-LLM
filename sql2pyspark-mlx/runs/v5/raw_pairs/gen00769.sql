select
    i.id,
    i.quantity,
    i.store_id,
    ss.store_id as store_sale_store_id
from inventory i
left outer join store_sales ss on i.store_id = ss.store_id and i.product_id = ss.product_id
where ss.revenue >= 3400 and i.product_id in (7, 8, 9)
