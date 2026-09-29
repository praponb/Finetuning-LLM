select s.city, max(i.quantity) - min(i.quantity) as quantity_range
from inventory i
inner join stores s on i.store_id = s.id
group by s.city
