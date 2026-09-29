select * from (select status, channel, shipping_fee from orders)
pivot (max(shipping_fee) for channel in ('web', 'partner', 'mobile'));
