select quarter(ss.sale_date) as sale_quarter, count(i.quantity) as quantity_count
from inventory i
left outer join store_sales ss on i.store_id = ss.store_id and i.product_id = ss.product_id
where ss.product_id = 4
group by quarter(ss.sale_date)
order by quantity_count desc, sale_quarter desc
