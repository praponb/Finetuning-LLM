select
    date_trunc('year', txn_time) as year_start,
    round(avg(amount), 2) as avg_amount,
    sort_array(collect_set(merchant)) as merchant_list,
    count(*) filter (where txn_type = 'DEBIT') as debit_rows,
    max(txn_time) as last_seen
from transactions
group by date_trunc('year', txn_time)
order by year_start asc;
