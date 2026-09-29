select s.id, s.city, count(i.id) as inventory_count
from stores s
left join inventory i on i.store_id = s.id
where s.region is not null
group by s.id, s.city
order by inventory_count desc, id, city
limit 10
