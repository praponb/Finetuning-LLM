select
    i.quantity,
    i.product_id,
    ss.units,
    ss.revenue,
    ss.sale_date
from inventory i
left outer join store_sales ss on i.store_id = ss.store_id and i.product_id = ss.product_id
order by ss.revenue desc;
