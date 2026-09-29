select product_id, count(distinct quantity) as unique_products
from inventory
where store_id in (3, 4)
group by product_id
order by 2 desc
