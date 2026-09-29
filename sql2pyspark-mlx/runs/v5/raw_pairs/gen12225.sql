select
    case when amount >= 1290 then 'high' when amount >= 630 then 'mid' else 'low' end as amount_bucket,
    status,
    avg(amount) as avg_amount,
    min(paid_at) as first_paid_at
from payments
where paid_at < '2025-04-01'
group by 1, 2;
