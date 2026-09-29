select
    date_trunc('MONTH', customers.signup_date) as signup_month,
    customers.country,
    count(distinct orders.amount) as unique_stores,
    count(*) as total_count
from orders
inner join customers on orders.customer_id = customers.id
where customers.email like '%@gmail.com'
group by date_trunc('MONTH', customers.signup_date), customers.country
order by customers.country, signup_month
limit 5;
