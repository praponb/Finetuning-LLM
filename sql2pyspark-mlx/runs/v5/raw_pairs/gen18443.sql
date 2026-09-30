select a.id, a.opened_date, a.account_type, a.status from accounts a where a.customer_id in (select id from customers where country != 'JP');
