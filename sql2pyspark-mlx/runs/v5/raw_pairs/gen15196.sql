select * from (select channel, status, amount from orders)
pivot (sum(amount) for status in ('SHIPPED', 'RETURNED'));
