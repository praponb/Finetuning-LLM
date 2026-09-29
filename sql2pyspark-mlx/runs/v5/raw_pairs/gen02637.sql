select status, sum(case when status = 'CLOSED' then balance else 0 end) as closed_balance
from accounts
group by 1
having sum(case when status = 'CLOSED' then balance else 0 end) < 69
