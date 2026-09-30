select p.id, p.product_name, count(ss.id) as store_sale_count
from products p
inner join store_sales ss on p.id = ss.product_id
group by p.id, p.product_name
having count(ss.id) > 5
order by product_name
