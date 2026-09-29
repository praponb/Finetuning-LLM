select
    i.id,
    i.product_id,
    i.quantity,
    ss.revenue,
    year(ss.sale_date) as sale_date_year
from inventory i
left join store_sales ss on i.store_id = ss.store_id and i.product_id = ss.product_id
where i.product_id in (2, 4);
