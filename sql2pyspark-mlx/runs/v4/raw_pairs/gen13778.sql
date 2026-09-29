select status, opened_date
from accounts
where account_type not in ('SAVINGS', 'CHECKING', 'LOAN')
union all
select status, opened_date
from accounts
where balance between 35000 and 54000
