select
    method,
    sum(amount) as total_amount,
    count(*) as cnt,
    sum(sum(amount)) over (order by method) as cumulative_amount
from payments
where status = 'REFUNDED'
group by method
order by total_amount desc, method
limit 10;
