select status
from accounts
union
select status
from payments
where status <> 'SUCCESS'
