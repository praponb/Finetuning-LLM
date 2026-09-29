select date_trunc('MONTH', signup_date) as signup_month, count(*) as num_customers, sum(age) as total_age
from customers
where signup_date < '2025-01-09' and segment not in ('SMB', 'Corporate', 'Consumer')
group by date_trunc('MONTH', signup_date)
order by num_customers desc, total_age desc;
