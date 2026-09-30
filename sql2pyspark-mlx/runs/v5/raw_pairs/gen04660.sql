select
    s.id,
    s.customer_id,
    c.email,
    c.signup_date
from subscriptions s
left outer join customers c on s.customer_id = c.id
where c.segment is not null
order by c.email, s.id;
