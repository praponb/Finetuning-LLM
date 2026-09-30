select
    date_format(paid_at, 'yyyy-MM') as paid_month,
    sum(amount) as total_amount,
    count(*) as cnt,
    sum(sum(amount)) over (order by date_format(paid_at, 'yyyy-MM')) as cumulative_amount
from payments
where status in ('SUCCESS', 'FAILED', 'REFUNDED')
group by date_format(paid_at, 'yyyy-MM')
order by cumulative_amount
