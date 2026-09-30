with cleaned as (
    -- Keep recent, valid rows
    select
        id,
        account_id,
        txn_type,
        amount,
        txn_time,
        merchant,
        is_flagged,
        ceil(amount) as amount_whole,
        if(is_flagged, 'Yes', 'No') as is_flagged_label
    from transactions
    where txn_time >= date '2023-04-10'
      and txn_type not in ('DEBIT', 'REFUND')
)
-- Running and moving aggregates
select
    id,
    amount,
    txn_time,
    is_flagged,
    is_flagged_label,
    sum(amount) over (partition by account_id order by amount, id rows between unbounded preceding and current row) as running_amount,
    row_number() over (partition by merchant order by amount_whole desc, id) as event_seq
from cleaned
where account_id in (3, 16, 27);
