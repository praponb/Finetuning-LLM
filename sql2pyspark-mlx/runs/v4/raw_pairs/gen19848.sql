select status
from orders
where store_id is not null
except
select status
from accounts
where status in ('OPEN', 'CLOSED', 'FROZEN');
