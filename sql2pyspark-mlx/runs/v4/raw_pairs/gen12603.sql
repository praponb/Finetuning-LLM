select
    s.id,
    s.customer_id,
    s.country,
    s.device
from sessions s
where s.customer_id in (select id from customers where segment in ('Consumer', 'Corporate', 'SMB'));
