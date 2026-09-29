with matched_keys as (
    select merchant, id
    from transactions
    intersect
    select merchant, id
    from transactions
    where not is_flagged
),
verified as (
    select s.*
    from transactions s
    inner join matched_keys k on s.merchant = k.merchant and s.id = k.id
)
select
    id,
    account_id,
    txn_type,
    amount,
    txn_time,
    merchant
from verified
order by merchant;
