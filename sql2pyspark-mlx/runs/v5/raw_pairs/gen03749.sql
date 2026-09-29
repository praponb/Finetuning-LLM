select
    s.id,
    s.customer_id,
    s.end_date,
    s.plan,
    c.city
from subscriptions s
inner join customers c on s.customer_id = c.id
where s.monthly_fee > 100
order by id
