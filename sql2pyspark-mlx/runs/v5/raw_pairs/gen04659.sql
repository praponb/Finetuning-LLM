select c.is_active, count(distinct p.amount) unique_statuss
from payments p
full outer join orders o on p.order_id = o.id
inner join customers c on c.id = o.customer_id
group by c.is_active
having count(distinct p.amount) > 3
order by 2, is_active
limit 3;
