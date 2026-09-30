select
    paid_month,
    method,
    latest_paid_at,
    avg_amount
from (
    select
        date_format(paid_at, 'yyyy-MM') as paid_month,
        method,
        max(paid_at) as latest_paid_at,
        avg(amount) as avg_amount
    from payments
    where status in ('SUCCESS', 'REFUNDED', 'FAILED')
    group by date_format(paid_at, 'yyyy-MM'), method
) as sub
where avg_amount < 1150;
