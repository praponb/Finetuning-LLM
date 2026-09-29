select s.city, sum(p.amount) total_amount, max(p.amount) - min(p.amount) amount_range
from payments p
right join orders o on p.order_id = o.id
inner join stores s on o.store_id = s.id
group by s.city
order by 3 desc, city desc;
