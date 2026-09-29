select opened_date
from accounts
where customer_id is not null and status in ('FROZEN', 'OPEN');
