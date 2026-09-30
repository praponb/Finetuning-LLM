select status
from accounts
intersect
select status
from payments
order by status;
