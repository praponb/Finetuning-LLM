with cleaned as (
    -- Filter the raw rows and derive helper columns
    select
        *,
        date_format(txn_time, 'yyyy-MM-dd') as txn_time_label,
        coalesce(merchant, 'unassigned') as merchant_filled
    from transactions
    where txn_time > to_timestamp('2024-05-19 02:00:00')
      and hour(txn_time) between 10 and 17
)
select account_id
from cleaned
intersect
select account_id
from transactions
where txn_type in ('FEE', 'REFUND')
