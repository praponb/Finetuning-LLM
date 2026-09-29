select
    id,
    status,
    opened_date,
    balance,
    lag(balance, 1) over (partition by status order by opened_date, id) as prev_balance
from accounts;
