select case when o.status in ('CANCELLED', 'SHIPPED') then 'core' else 'other' end status_group, max(s.cost) max_cost
from shipments s
join orders o on s.order_id = o.id
left join customers c on o.customer_id = c.id
where c.credit_limit is not null
group by case when o.status in ('CANCELLED', 'SHIPPED') then 'core' else 'other' end
