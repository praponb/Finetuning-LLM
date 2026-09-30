select quarter(ss.sale_date) as sale_quarter, round(stddev(i.quantity), 2) as stddev_quantity
from inventory i
left outer join store_sales ss on i.store_id = ss.store_id and i.product_id = ss.product_id
where ss.product_id = 5
group by quarter(ss.sale_date)
order by sale_quarter desc
