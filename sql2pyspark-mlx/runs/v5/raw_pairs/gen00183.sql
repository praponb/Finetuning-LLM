select quarter(o.order_date) as order_quarter, avg(p.amount) as avg_amount, min(p.amount) as min_amount
from payments p
inner join orders o on p.order_id = o.id
inner join customers c on o.customer_id = c.id
where c.age <= 51
group by quarter(o.order_date)
order by min_amount
