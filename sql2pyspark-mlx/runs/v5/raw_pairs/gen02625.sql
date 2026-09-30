select id, status, discount, channel, amount, rank() over (partition by status order by discount desc) as discount_rank, avg(amount) over (partition by channel) as channel_avg_amount from orders;
