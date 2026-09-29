select
    c.signup_date,
    c.credit_limit,
    s.country,
    s.customer_id,
    s.device
from customers c
left join sessions s on c.id = s.customer_id
where s.browser in ('Edge', 'Safari')
order by device desc
