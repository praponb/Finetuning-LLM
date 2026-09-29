select product_id, count(distinct quantity) as unique_products
from inventory
where store_id in (2, 3) and last_restock between '2024-05-15' and '2025-06-29'
group by product_id
order by product_id
