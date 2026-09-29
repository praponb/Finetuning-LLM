select
    hour(txn_time) as txn_hour,
    case when amount >= 4000 then 'high' when amount >= 700 then 'mid' else 'low' end as amount_bucket,
    avg(amount) as avg_amount,
    round(stddev(amount), 2) as stddev_amount
from transactions
group by 1, 2
having avg(amount) > 2000
order by txn_hour desc, amount_bucket
limit 50
