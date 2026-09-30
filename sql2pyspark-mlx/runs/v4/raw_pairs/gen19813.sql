select status, 'order' as source
from orders
where amount is null
union
select status, 'account' as source
from accounts
