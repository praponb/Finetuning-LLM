select s.id, s.started_at, s.device
from sessions s
where s.customer_id not in (select id from customers where segment in ('Consumer', 'Corporate', 'SMB'))
order by device desc, id desc
limit 5;
