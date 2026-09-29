select * from (select status, channel, amount from orders) pivot (avg(amount) for channel in ('web', 'store', 'partner', 'mobile'));
