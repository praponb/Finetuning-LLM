select
    id,
    merchant,
    txn_time,
    rank() over (partition by merchant order by txn_time) as txn_time_rank
from transactions
where merchant != 'Lazada'
order by txn_time
