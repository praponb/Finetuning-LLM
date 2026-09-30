with filtered as (
    select
        *,
        to_date(txn_time) as txn_time_date,
        coalesce(merchant, 'NONE') as merchant_clean
    from transactions
    where account_id in (11, 16, 18)
),
ranked as (
    select *
    from (
        select *, row_number() over (partition by account_id order by amount nulls last, id) as rn
        from filtered
    ) x
    where rn = 1
),
prev_events as (
    select *, lag(txn_time) over (partition by account_id order by txn_time, id) as prev_txn_time
    from ranked
),
gaps as (
    select *, case when prev_txn_time is null or txn_time > prev_txn_time + interval '30' day then 1 else 0 end as session_start_flag
    from prev_events
),
sessionized as (
    select *, sum(session_start_flag) over (partition by account_id order by txn_time, id rows between unbounded preceding and current row) as session_no
    from gaps
),
latest as (
    select *
    from sessionized
    qualify row_number() over (partition by merchant, account_id order by rn, id) = 1
)
select
    id,
    account_id,
    txn_type,
    txn_time,
    merchant,
    txn_time_date,
    merchant_clean,
    rn,
    prev_txn_time
from latest
order by prev_txn_time desc nulls last;
