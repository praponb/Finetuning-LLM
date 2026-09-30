select quarter(txn_time) as txn_quarter, max(amount) - min(amount) as amount_range
from transactions
where account_id in (14, 26, 36, 37) and merchant in ('Grab', 'Shell')
group by txn_quarter;
