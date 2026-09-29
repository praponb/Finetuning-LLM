with cleaned as (
    -- Filter the raw rows and derive helper columns
    select
        *,
        cast(is_flagged as int) is_flagged_flag,
        cast(amount as int) amount_int,
        coalesce(merchant, 'OTHER') resolved_merchant
    from transactions
    where txn_time > timestamp '2023-09-27 11:00:00'
      and merchant is not null
)
select id, account_id
from cleaned
intersect
select id, account_id
from transactions
where is_flagged
