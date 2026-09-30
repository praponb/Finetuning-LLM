select status, 'account' as source
from accounts
where customer_id in (20, 21, 36)
union all
select status, 'payment' as source
from payments
order by status;
