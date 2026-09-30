select i.id, i.quantity, s.store_name
from inventory i
inner join stores s on i.store_id = s.id
order by quantity desc, id asc
limit 1;
