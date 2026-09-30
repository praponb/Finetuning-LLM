select o.id, o.status, t.unique_methods
from orders o
inner join (select order_id, count(distinct amount) as unique_methods
    from payments
    group by order_id) t on o.id = t.order_id
order by 1 desc
limit 1
