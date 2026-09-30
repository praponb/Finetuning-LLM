select
    status,
    sum(discount) as total_discount,
    count(*) as cnt,
    sum(sum(discount)) over (order by status) as cumulative_discount
from orders
group by status
order by status, cnt desc
limit 20;
