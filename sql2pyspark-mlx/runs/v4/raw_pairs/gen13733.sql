select quarter(order_date) as order_quarter, max(amount) - min(amount) as amount_range
from orders
group by quarter(order_date)
having amount_range > 71
order by amount_range;
