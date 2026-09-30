select accounts.id, accounts.customer_id from accounts where accounts.id in (select account_id from transactions) order by customer_id desc
