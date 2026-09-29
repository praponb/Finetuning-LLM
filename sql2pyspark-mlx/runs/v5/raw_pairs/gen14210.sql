select * from (select status, account_type, balance from accounts) pivot (count(balance) for account_type in ('CHECKING', 'LOAN', 'CREDIT', 'SAVINGS'));
