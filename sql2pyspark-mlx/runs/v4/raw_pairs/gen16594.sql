select t.id, t.account_id, a.status, a.account_type, a.balance from transactions t inner join accounts a on t.account_id = a.id where a.customer_id is not null and a.customer_id is not null
