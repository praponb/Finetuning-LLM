select status, min(balance) as min_balance
from accounts
where account_type in ('SAVINGS', 'CHECKING', 'CREDIT') and opened_date > '2025-05-01'
group by status
order by 2;
