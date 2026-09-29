select t.amount, a.status, a.account_type, c.city, c.country from transactions t right join accounts a on t.account_id = a.id inner join customers c on c.id = a.customer_id order by city desc;
