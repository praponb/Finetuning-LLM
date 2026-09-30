select
    date_trunc('MONTH', paid_at) as paid_month,
    method,
    sum(case when status = 'REFUNDED' then 1 else 0 end) as refunded_count,
    round(stddev(amount), 2) as stddev_amount
from payments
where order_id = 6 and order_id is null
group by date_trunc('MONTH', paid_at), method;
