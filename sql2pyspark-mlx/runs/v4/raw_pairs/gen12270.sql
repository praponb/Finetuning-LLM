select
    p.stock_qty,
    ss.store_id,
    ss.revenue,
    ss.sale_date
from products p
left join store_sales ss on p.id = ss.product_id
order by ss.revenue;
