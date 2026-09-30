select * from (select customer_id, account_type, balance from accounts) src pivot (max(balance) for account_type in ('LOAN' as loan, 'SAVINGS' as savings, 'CREDIT' as credit))
