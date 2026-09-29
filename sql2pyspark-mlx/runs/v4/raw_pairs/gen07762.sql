select
    method,
    to_date(paid_at) paid_day,
    status,
    avg(amount) avg_amount,
    round((100 * sum(case when method = 'promptpay' then 1 else 0 end)) / count(*), 1) pct_promptpay
from payments
where (status != 'REFUNDED' and hour(paid_at) between 11 and 18) or paid_at >= '2025-08-29'
group by method, to_date(paid_at), status
order by avg_amount desc
