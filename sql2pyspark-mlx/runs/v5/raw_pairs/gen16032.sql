select s.city, round(stddev(i.quantity), 2) as stddev_quantity, min(i.last_restock) as first_last_restock
from inventory i
inner join stores s on i.store_id = s.id
group by s.city
order by first_last_restock desc
