select c.country, c.is_active, a.status, a.account_type, a.customer_id from customers c left join accounts a on c.id = a.customer_id where a.account_type in ('CHECKING', 'LOAN', 'CREDIT');
