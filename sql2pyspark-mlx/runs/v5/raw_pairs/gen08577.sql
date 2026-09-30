select txn_month, min_amount
from (
    select month(txn_time) as txn_month, min(amount) as min_amount
    from transactions
    where merchant = 'Grab'
    group by month(txn_time)
) as agg
where min_amount > 2600
order by min_amount desc, txn_month
limit 20;
