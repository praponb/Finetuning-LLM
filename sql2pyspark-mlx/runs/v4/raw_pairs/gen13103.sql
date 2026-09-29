select
    is_flagged,
    month(txn_time) as txn_month,
    sort_array(collect_set(merchant)) as merchant_list,
    concat_ws(', ', sort_array(collect_set(txn_type))) as txn_type_values
from transactions
group by is_flagged, month(txn_time)
order by txn_type_values, txn_month;
