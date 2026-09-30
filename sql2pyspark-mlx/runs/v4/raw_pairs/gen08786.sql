select channel, sum(amount) as total_amount, sum(sum(amount)) over (order by channel) as cumulative_amount from orders group by channel order by cumulative_amount desc;
