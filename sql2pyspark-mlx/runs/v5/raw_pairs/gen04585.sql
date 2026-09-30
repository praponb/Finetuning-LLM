select status, account_type, count(balance) as balance_count
from accounts
where account_type in ('CREDIT', 'SAVINGS')
group by status, account_type;
