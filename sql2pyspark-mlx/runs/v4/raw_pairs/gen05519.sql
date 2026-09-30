select id, signup_date, city from customers where email like '%@startup.io' or signup_date between '2023-04-01' and '2024-03-01' order by id limit 10
