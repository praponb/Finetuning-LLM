with cleaned as (
    -- Basic cleansing and defaults
    select
        id,
        account_id,
        amount,
        merchant,
        is_flagged,
        cast(txn_time as date) as txn_time_day,
        initcap(trim(merchant)) as merchant_title
    from transactions
    where txn_time > timestamp '2024-02-20 15:00:00'
      and merchant is not null
)
-- Attach accounts attributes
select a.*, p.account_type
from cleaned a
left join accounts p
    on a.account_id = p.id;
