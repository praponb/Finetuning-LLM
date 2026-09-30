select a.id, a.status, a.balance, a.customer_id, c.age, c.email, c.signup_date from accounts a inner join customers c on a.customer_id = c.id where c.age is not null order by a.status, c.signup_date
