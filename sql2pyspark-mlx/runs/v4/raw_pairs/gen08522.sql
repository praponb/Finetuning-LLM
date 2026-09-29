select
    txn_type,
    txn_month,
    transaction_count,
    latest_txn_time
from (
    select
        txn_type,
        date_format(txn_time, 'yyyy-MM') as txn_month,
        count(*) as transaction_count,
        max(txn_time) as latest_txn_time
    from transactions
    group by txn_type, date_format(txn_time, 'yyyy-MM')
) as sub
where transaction_count > 6
order by txn_type, latest_txn_time;
