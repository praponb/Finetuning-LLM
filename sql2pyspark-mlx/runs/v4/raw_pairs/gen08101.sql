select * from (select customer_id, account_type, balance from accounts) pivot (count(balance) for account_type in ('SAVINGS', 'LOAN')) order by customer_id
