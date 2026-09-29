with with_prev as (
    -- Previous event time per key
    select *, lag(paid_at) over (partition by method order by paid_at asc, id asc) as prev_paid_at
    from payments
),
flagged as (
    -- Flag rows that start a new session
    select *, case when prev_paid_at is null or paid_at > prev_paid_at + interval '30' day then 1 else 0 end as is_new_session
    from with_prev
),
sessions_payments as (
    -- Running sum of the flag = session number
    select *, sum(is_new_session) over (partition by method order by paid_at asc, id asc rows between unbounded preceding and current row) as burst_id
    from flagged
)
select
    id,
    method,
    amount,
    paid_at,
    status,
    prev_paid_at,
    is_new_session,
    burst_id,
    case when method rlike '^(card|promptpay)$' then 'CORE' else 'EXTENDED' end as method_zone
from sessions_payments
