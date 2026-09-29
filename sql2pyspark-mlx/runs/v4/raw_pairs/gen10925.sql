select * from (select status, channel, amount from orders) pivot (avg(amount) for channel in ('store', 'partner', 'web', 'mobile'))
