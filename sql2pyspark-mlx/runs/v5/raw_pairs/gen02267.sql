select
    account_type,
    avg(balance) as avg_balance,
    round(stddev(balance), 2) as stddev_balance,
    count(*) as cnt
from accounts
where balance > 50000
group by account_type;
