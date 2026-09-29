select channel, status, round(stddev(discount), 2) stddev_discount
from orders
where customer_id is null
group by channel, status
order by status desc
