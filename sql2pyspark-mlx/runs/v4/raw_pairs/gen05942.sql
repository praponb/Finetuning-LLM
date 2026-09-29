select s.city, sum(i.quantity) as total_quantity
from inventory i
right join products p on i.product_id = p.id
left join stores s on i.store_id = s.id
where s.sqft < 14574
group by s.city
order by s.city;
