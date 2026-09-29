select
    id,
    txn_type,
    amount,
    row_number() over (partition by txn_type order by amount desc, id) as amount_row
from transactions
where merchant not in ('7-Eleven', 'Shell')
