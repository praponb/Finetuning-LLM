select ss.product_id, quarter(ss.sale_date) as sale_quarter, sum(i.quantity) as total_quantity
from inventory i
left join store_sales ss on i.store_id = ss.store_id and i.product_id = ss.product_id
group by ss.product_id, quarter(ss.sale_date)
having sum(i.quantity) > 410
order by sale_quarter desc;
