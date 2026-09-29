select a.id, a.account_type, a.customer_id, c.age from accounts a inner join customers c on a.customer_id = c.id;
