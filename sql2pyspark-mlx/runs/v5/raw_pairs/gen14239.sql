select i.last_restock, i.product_id, ss.sale_date
from inventory i
left join store_sales ss on i.store_id = ss.store_id and i.product_id = ss.product_id
order by last_restock, i.product_id desc;
