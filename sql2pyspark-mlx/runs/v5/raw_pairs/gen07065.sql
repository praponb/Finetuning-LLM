select ss.product_id, count(i.quantity) as quantity_count
from inventory i
left outer join store_sales ss on i.store_id = ss.store_id and i.product_id = ss.product_id
where i.quantity <= 226
group by ss.product_id
order by ss.product_id desc
limit 50;
