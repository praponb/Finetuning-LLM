select customers.segment, count(distinct accounts.balance) as unique_customers
from accounts
join customers on accounts.customer_id = customers.id
group by customers.segment
having unique_customers >= 5
order by segment desc, unique_customers asc
limit 10;
