select
    i.quantity,
    i.product_id,
    ss.store_id,
    ss.units,
    ss.revenue
from inventory i
join store_sales ss on i.store_id = ss.store_id and i.product_id = ss.product_id
where ss.revenue is not null
