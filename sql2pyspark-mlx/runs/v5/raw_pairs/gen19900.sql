select *, round(balance * 1.07, 2) as adjusted_balance, upper(status) as status_upper
from accounts
order by status, customer_id
