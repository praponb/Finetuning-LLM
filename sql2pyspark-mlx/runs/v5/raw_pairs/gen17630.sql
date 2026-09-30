select
    store_id,
    product_id,
    min(quantity) as min_quantity,
    avg(quantity) as avg_quantity
from inventory
where product_id in (6, 7, 9, 10)
group by store_id, product_id
having avg_quantity >= 160
order by store_id, product_id
limit 10
