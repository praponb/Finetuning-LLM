select
    status,
    sum(monthly_fee) total_monthly_fee,
    count(*) cnt,
    rank() over (order by sum(monthly_fee) desc) as monthly_fee_rank
from subscriptions
where month(end_date) in (5, 6, 8)
group by status
