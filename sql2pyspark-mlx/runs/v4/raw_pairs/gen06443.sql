select year(c.signup_date) as signup_year, avg(o.discount) as avg_discount, max(o.order_date) as latest_order_date
from orders o
join stores s on o.store_id = s.id
left outer join customers c on c.id = o.customer_id
where o.status in ('COMPLETED', 'CANCELLED', 'RETURNED')
group by year(c.signup_date)
order by avg_discount desc, signup_year
limit 5
