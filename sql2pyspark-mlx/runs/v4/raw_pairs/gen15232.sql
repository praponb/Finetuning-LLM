select ss.store_id, max(i.last_restock) as latest_last_restock
from inventory i
inner join store_sales ss on i.store_id = ss.store_id and i.product_id = ss.product_id
where i.last_restock between '2023-04-01' and '2023-11-26'
group by ss.store_id
order by store_id
limit 50;
