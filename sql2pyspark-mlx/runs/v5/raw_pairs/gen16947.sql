select c.id, c.segment
from customers c
where not exists (select 1 from sessions s where s.customer_id = c.id and to_date(s.started_at) >= '2023-06-01');
