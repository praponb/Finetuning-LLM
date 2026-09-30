select
    *,
    row_number() over (order by balance, id) as row_seq,
    ntile(10) over (order by balance, id) as balance_bucket,
    min(balance) over (partition by customer_id) as customer_id_min_balance
from accounts;
