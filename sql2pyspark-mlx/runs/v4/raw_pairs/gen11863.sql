select account_id, concat_ws(', ', sort_array(collect_set(txn_type))) as txn_type_values, min(txn_time) as min_txn_time
from transactions
where (amount >= 3800 and txn_type <> 'REFUND') or amount < 1800
group by account_id
order by min_txn_time desc, account_id
limit 10
