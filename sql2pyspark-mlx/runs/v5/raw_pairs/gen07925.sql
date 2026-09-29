select a.id, a.account_type, a.balance, a.customer_id from accounts a where a.customer_id in (19, 33) and a.customer_id in (select id from customers where segment not in ('SMB', 'Corporate'));
