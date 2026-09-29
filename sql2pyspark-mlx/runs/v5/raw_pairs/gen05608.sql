select status, channel, max(amount) as max_amount from orders group by 1, 2;
