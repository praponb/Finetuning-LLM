select a.id, a.account_type from accounts a where a.customer_id not in (select id from customers where email like '%@company.co') order by a.id;
