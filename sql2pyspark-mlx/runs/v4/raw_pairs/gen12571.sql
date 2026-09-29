select method, sum(amount) as total_amount, sum(sum(amount)) over (order by method) as cumulative_amount
from payments
group by method
order by total_amount desc;
