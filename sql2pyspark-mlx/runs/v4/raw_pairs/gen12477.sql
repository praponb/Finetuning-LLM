select * from (select account_type, status, balance from accounts) pivot (sum(balance) for status in ('FROZEN', 'OPEN'))
