select store_id, product_id, count(quantity) as quantity_count
from inventory
where product_id = 1
group by store_id, product_id
having count(quantity) >= 4
order by store_id desc
