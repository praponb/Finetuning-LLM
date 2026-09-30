select date_format(ss.sale_date, 'yyyy-MM') as sale_month, round(avg(i.quantity), 2) as avg_quantity
from inventory i
left outer join store_sales ss on i.store_id = ss.store_id and i.product_id = ss.product_id
group by date_format(ss.sale_date, 'yyyy-MM')
order by avg_quantity
