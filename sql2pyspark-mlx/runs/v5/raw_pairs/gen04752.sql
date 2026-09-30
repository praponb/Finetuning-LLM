with base as (
    -- Filter the raw rows and derive helper columns
    select
        id,
        account_id,
        txn_type,
        amount,
        txn_time,
        is_flagged,
        substring(merchant, 1, 3) as merchant_prefix,
        lower(txn_type) as txn_type_lower
    from transactions
    where account_id in (8, 29, 35, 36)
      and merchant is not null
),
gaps as (
    -- Flag rows that start a new session (gap > interval)
    select *, case when txn_time - lag(txn_time) over (partition by account_id order by txn_time, id) > interval 7 days then 1 else 0 end as is_new_burst
    from base
),
islands as (
    -- Running sum of the flag = session number
    select *, sum(is_new_burst) over (partition by account_id order by txn_time, id rows between unbounded preceding and current row) as session_id
    from gaps
),
enriched as (
    -- Running and moving aggregates
    select
        *,
        avg(session_id) over (order by txn_time, id rows between 6 preceding and 1 preceding) as session_id_avg_6_prior,
        sum(session_id) over (partition by txn_type order by is_new_burst, id rows between unbounded preceding and current row) as running_session_id
    from islands
),
scored as (
    -- Window metrics over several different partitions
    select
        *,
        row_number() over (partition by account_id order by amount desc, id) as row_seq,
        round(amount / sum(amount) over (partition by account_id, txn_type), 4) as amount_share
    from enriched
    where session_id_avg_6_prior != 2
),
flagged as (
    -- Flag rows that start a new session (gap > interval)
    select *, case when lag(txn_time) over (partition by txn_type order by txn_time, id) is null
                 or txn_time > lag(txn_time) over (partition by txn_type order by txn_time, id) + interval 30 days then 1 else 0 end as is_new_session
    from scored
),
sessionized as (
    -- Running sum of the flag = session number
    select *, sum(is_new_session) over (partition by txn_type order by txn_time, id rows between unbounded preceding and current row) as session_no
    from flagged
),
with_dims as (
    -- Attach accounts attributes
    select
        a.*,
        p2.balance,
        p2.account_type
    from sessionized a
    left join accounts p2
        on a.account_id = p2.id
)
select
    s.id,
    s.account_id,
    s.txn_type,
    s.txn_time,
    s.is_flagged,
    s.merchant_prefix,
    s.is_new_burst,
    s.session_id_avg_6_prior,
    s.running_session_id,
    case when txn_type rlike '^(CREDIT|REFUND)$' then 'CORE' when txn_type rlike '^(DEBIT|FEE)$' then 'OTHER_ZONE' else 'EXTENDED' end as txn_type_zone,
    (select max(t2.amount) from transactions t2 where t2.account_id = s.account_id) as max_transaction_amount
from with_dims s
order by account_id nulls last
