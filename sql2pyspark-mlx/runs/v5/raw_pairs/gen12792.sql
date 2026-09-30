select
    status,
    round(stddev(balance), 2) stddev_balance,
    min(balance) min_balance,
    avg(balance) avg_balance
from accounts
group by 1
order by min_balance desc
