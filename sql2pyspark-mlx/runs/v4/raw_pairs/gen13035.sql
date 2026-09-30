select is_active, sum(age) as total_age, count(*) as cnt, rank() over (order by sum(age) desc) as age_rank from customers group by is_active order by total_age;
