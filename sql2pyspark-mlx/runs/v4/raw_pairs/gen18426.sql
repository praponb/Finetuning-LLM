select s.city, count(i.id) as inventory_count
from stores s
left outer join inventory i on s.id = i.store_id
group by s.city
