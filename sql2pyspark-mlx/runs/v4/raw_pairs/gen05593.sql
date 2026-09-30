select id, signup_date, age, lag(age, 1) over (order by signup_date, id) as prev_age from customers;
