with remaining_keys as (
    select account_id, id
    from transactions
    intersect
    select account_id, id
    from transactions
    where hour(txn_time) between 12 and 17
),
verified as (
    select s.*
    from transactions s
    inner join remaining_keys k on s.account_id = k.account_id and s.id = k.id
)
select
    s.account_id,
    s.txn_type,
    s.amount,
    s.txn_time,
    s.merchant,
    s.is_flagged,
    (select max(t2.amount) from transactions t2 where t2.account_id = s.account_id) as max_transaction_amount
from verified s
