select a.id, a.status, a.balance, c.is_active from accounts a inner join customers c on a.customer_id = c.id where c.name is null order by id desc
