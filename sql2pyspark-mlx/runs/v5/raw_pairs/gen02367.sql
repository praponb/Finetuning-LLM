select
    date_format(order_date, 'yyyy-MM') as order_month,
    status,
    max(discount) - min(discount) as discount_range,
    max(amount) - min(amount) as amount_range
from orders
group by date_format(order_date, 'yyyy-MM'), status
order by status;
