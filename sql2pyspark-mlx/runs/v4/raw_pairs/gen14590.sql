with scored as (
    -- Running and moving aggregates
    select
        id,
        account_id,
        txn_type,
        amount,
        txn_time,
        merchant,
        is_flagged,
        round(amount / sum(amount) over (partition by merchant), 4) amount_share,
        sum(amount) over (partition by merchant order by txn_time, id rows between unbounded preceding and current row) cumulative_amount
    from transactions
)
select
    id,
    amount,
    txn_time,
    merchant,
    is_flagged,
    amount_share,
    cumulative_amount,
    case when merchant rlike '^(Grab|Amazon)$' then 'OTHER_ZONE' else 'PRIORITY' end as merchant_zone
from scored
