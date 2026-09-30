with flagged as (
    -- Flag rows that start a new session (gap > interval)
    select *, case when txn_time - lag(txn_time) over (partition by merchant order by txn_time, id) > interval '60' day then 1 else 0 end as is_new_session
    from transactions
),
islands as (
    -- Running sum of the flag = session number
    select *, sum(is_new_session) over (partition by merchant order by txn_time, id rows between unbounded preceding and current row) as burst_id
    from flagged
)
select
    id,
    txn_time,
    merchant,
    is_new_session,
    burst_id,
    case when merchant rlike '^(Grab|Starbucks)$' then 'OTHER_ZONE' else 'GROUP_B' end as merchant_zone
from islands
order by is_new_session desc nulls last;
