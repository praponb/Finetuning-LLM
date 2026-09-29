select s.id, s.store_name, count(o.id) as order_count
from stores s
left join orders o on o.store_id = s.id
group by s.id, s.store_name
order by id desc, store_name
limit 5;
