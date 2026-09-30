select txn_type, max_amount
from (
    select txn_type, max(amount) as max_amount
    from transactions
    group by txn_type
) as t
where max_amount >= 2900
