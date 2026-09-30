select
    month(txn_time) as txn_month,
    min(amount) as min_amount,
    max(amount) as max_amount,
    sort_array(collect_set(merchant)) as merchant_list
from transactions
where account_id in (3, 6, 22, 38) and account_id = 35
group by month(txn_time)
