select year(ss.sale_date) as sale_year, max(i.quantity) as max_quantity
from inventory i
left join store_sales ss on i.store_id = ss.store_id and i.product_id = ss.product_id
group by year(ss.sale_date)
order by max_quantity desc, sale_year
limit 3
