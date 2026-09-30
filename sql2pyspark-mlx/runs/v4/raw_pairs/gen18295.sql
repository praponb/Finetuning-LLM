select s.id, s.monthly_fee, s.plan
from subscriptions s
where s.customer_id not in (select id from customers where segment not in ('Consumer', 'SMB', 'Corporate'))
order by plan;
