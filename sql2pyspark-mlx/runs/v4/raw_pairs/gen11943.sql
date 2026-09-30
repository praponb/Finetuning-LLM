select c.id, c.name, count(a.id) as account_count from customers c inner join accounts a on c.id = a.customer_id where c.age < 31 group by c.id, c.name;
