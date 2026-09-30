select round((100 * sum(case when txn_type = 'FEE' then 1 else 0 end)) / count(*), 1) as pct_fee
from transactions
where account_id in (3, 18, 26, 35)
order by 1 desc;
