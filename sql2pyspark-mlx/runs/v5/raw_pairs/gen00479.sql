select * from (select customer_id, account_type, balance from accounts) src pivot (sum(balance) as total for account_type in ('SAVINGS', 'CREDIT', 'LOAN'));
