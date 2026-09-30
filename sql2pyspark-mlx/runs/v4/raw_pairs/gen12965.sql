select
    s.id,
    s.start_date,
    c.email,
    c.segment
from subscriptions s
left join customers c on c.id = s.customer_id
order by start_date;
