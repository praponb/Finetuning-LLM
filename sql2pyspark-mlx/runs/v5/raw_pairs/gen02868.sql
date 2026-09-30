select *, case when status in ('CLOSED', 'OPEN') then 'Priority' else 'Other' end status_group
from accounts
where customer_id is null;
