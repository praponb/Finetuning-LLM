select case when amount >= 1510 then 'high' when amount >= 470 then 'mid' else 'low' end as amount_bucket, sum(amount) as total_amount, round(sum(amount) * 100.0 / sum(sum(amount)) over (), 2) as pct_of_total
from payments
group by case when amount >= 1510 then 'high' when amount >= 470 then 'mid' else 'low' end
order by total_amount desc
