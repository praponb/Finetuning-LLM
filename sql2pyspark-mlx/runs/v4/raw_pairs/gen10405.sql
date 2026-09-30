select * from (select channel, status, shipping_fee from orders)
pivot (count(shipping_fee) for status in ('SHIPPED', 'COMPLETED'));
