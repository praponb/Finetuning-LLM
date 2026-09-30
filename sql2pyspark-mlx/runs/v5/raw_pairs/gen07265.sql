select merchant, round(stddev(amount), 2) as stddev_amount, min(amount) as min_amount
from transactions
where txn_type in ('DEBIT', 'REFUND') or is_flagged
group by merchant
having min(amount) < 3600
order by stddev_amount desc, merchant
limit 1
