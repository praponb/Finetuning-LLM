select
    plan,
    sum(monthly_fee) as total_monthly_fee,
    count(*) as cnt,
    dense_rank() over (order by sum(monthly_fee) desc) as rnk
from subscriptions
group by plan
order by cnt desc, total_monthly_fee desc;
