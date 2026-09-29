select
    i.id,
    i.quantity,
    i.last_restock,
    ss.sale_date,
    cast(ss.units as string) as units_num
from inventory i
inner join store_sales ss on i.store_id = ss.store_id and i.product_id = ss.product_id;
