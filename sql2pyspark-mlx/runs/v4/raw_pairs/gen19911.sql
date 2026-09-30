select merchant, sum(amount) total_amount, sum(sum(amount)) over (order by merchant) as cumulative_amount from transactions group by merchant order by total_amount desc;
