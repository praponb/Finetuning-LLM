select max(opened_date) as last_opened_date, max(balance) as max_balance
from accounts
where account_type = 'LOAN' and opened_date between '2023-06-06' and '2024-01-20'
order by last_opened_date
