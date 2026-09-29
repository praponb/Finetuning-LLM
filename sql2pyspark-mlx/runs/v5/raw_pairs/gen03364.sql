select
    method,
    date_format(paid_at, 'yyyy-MM') as paid_month,
    year(paid_at) as paid_year,
    avg(amount) as avg_amount
from payments
where paid_at < '2023-07-17' and order_id is null
group by method, date_format(paid_at, 'yyyy-MM'), year(paid_at)
order by paid_month asc
