select
    date_format(c.signup_date, 'yyyy-MM') as signup_month,
    c.country,
    sum(case when o.status = 'PENDING' then o.amount else 0 end) as pending_amount,
    max(o.amount) - min(o.amount) as amount_range
from orders o
join customers c on o.customer_id = c.id
where o.store_id is not null
group by date_format(c.signup_date, 'yyyy-MM'), c.country
order by signup_month asc, pending_amount desc;
