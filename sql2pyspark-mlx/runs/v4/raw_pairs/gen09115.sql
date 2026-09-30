select id, txn_time, rank() over (order by txn_time desc) as rnk
from transactions
