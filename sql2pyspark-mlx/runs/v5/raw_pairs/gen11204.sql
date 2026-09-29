select c.segment, count(case when p.status = 'SUCCESS' then 1 end) as success_count, count(*) as cnt
from payments p
join orders o on p.order_id = o.id
join customers c on o.customer_id = c.id
where o.order_date <= '2024-05-01'
group by c.segment
