select
    txn_type,
    case when amount >= 3200 then 'high' when amount >= 1300 then 'mid' else 'low' end amount_bucket,
    case when year(txn_time) < 2024 then 'before_2024' when year(txn_time) = 2024 then '2024' else 'after_2024' end txn_cohort,
    max(txn_time) last_txn_time
from transactions
where is_flagged or is_flagged
group by txn_type, case when amount >= 3200 then 'high' when amount >= 1300 then 'mid' else 'low' end, case when year(txn_time) < 2024 then 'before_2024' when year(txn_time) = 2024 then '2024' else 'after_2024' end
