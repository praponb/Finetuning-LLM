select a.id, a.opened_date, a.balance, a.account_type from accounts a where a.customer_id not in (select id from customers where segment = 'Corporate') order by a.opened_date, a.account_type desc
