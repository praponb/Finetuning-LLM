with base as (
    select
        *,
        coalesce(amount, 0) as amount_filled,
        datediff(current_date(), txn_time) as days_since_txn_time,
        coalesce(merchant, 'N/A') as merchant_clean
    from transactions
    where txn_time >= timestamp '2024-03-24 09:00:00'
)
select *
from (
    select *, row_number() over (partition by txn_type, merchant order by amount_filled desc nulls first, id) as rank_in_group
    from base
) x
where rank_in_group = 1
