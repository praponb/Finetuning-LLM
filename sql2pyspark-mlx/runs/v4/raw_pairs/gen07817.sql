select stores.city, max(inventory.quantity) as max_quantity, avg(inventory.quantity) as avg_quantity
from inventory
left join stores on inventory.store_id = stores.id
group by stores.city
order by max_quantity, avg_quantity desc
