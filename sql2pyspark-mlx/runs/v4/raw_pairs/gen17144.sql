select *
from accounts
where status in ('CLOSED', 'FROZEN') and customer_id = 12 and customer_id in (2, 37);
