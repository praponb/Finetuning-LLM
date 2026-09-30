with filtered as (
    -- Filter the raw rows and derive helper columns
    select
        *,
        upper(merchant) as merchant_upper,
        ceil(amount) as amount_whole
    from transactions
    where txn_time > timestamp '2023-10-14 03:00:00'
      and merchant in ('Lazada', '7-Eleven')
),
ranked as (
    -- QUALIFY-style window filter
    select *
    from filtered
    qualify row_number() over (partition by account_id, merchant order by amount_whole asc nulls first, id asc) = 1
),
with_gaps as (
    -- Flag rows that start a new session (gap > interval)
    select *, case when lag(txn_time) over (partition by merchant order by txn_time asc, id asc) is null
                 or txn_time > lag(txn_time) over (partition by merchant order by txn_time asc, id asc) + interval 7 days then 1 else 0 end as is_new_burst
    from ranked
),
sessionized as (
    -- Running sum of the flag = session number
    select *, sum(is_new_burst) over (partition by merchant order by txn_time asc, id asc rows between unbounded preceding and current row) as session_id
    from with_gaps
),
enriched as (
    -- Ranking and gap features
    select
        *,
        rank() over (partition by account_id order by session_id desc) as session_id_rank,
        first_value(amount, true) over (partition by account_id order by is_new_burst asc, id asc rows between unbounded preceding and current row) as first_known_amount,
        sum(is_new_burst) over w_part as running_is_new_burst
    from sessionized
    window w_part as (partition by merchant order by amount asc, id asc rows between unbounded preceding and current row)
),
metrics as (
    -- Running and moving aggregates
    select
        id,
        account_id,
        txn_type,
        amount,
        txn_time,
        merchant,
        is_flagged,
        merchant_upper,
        amount_whole,
        is_new_burst,
        session_id,
        session_id_rank,
        first_known_amount,
        running_is_new_burst,
        lag(amount, 1) over (partition by txn_type order by first_known_amount asc, id asc) as prev_amount,
        avg(session_id_rank) over (partition by account_id order by cast(txn_time as bigint) asc range between 2592000 preceding and current row) as session_id_rank_avg_30d,
        lag(amount_whole, 1) over (partition by merchant order by first_known_amount asc, id asc) as prev_amount_whole
    from enriched
),
valid as (
    -- Basic cleansing and defaults
    select
        id,
        txn_type,
        amount,
        merchant,
        is_flagged,
        merchant_upper,
        amount_whole,
        is_new_burst,
        session_id,
        first_known_amount,
        prev_amount,
        session_id_rank_avg_30d,
        amount - prev_amount_whole as amount_minus_prev_amount_whole,
        coalesce(merchant, 'N/A') as merchant_filled
    from metrics
    where txn_time > date '2024-04-01'
      and merchant is not null
),
base as (
    -- Filter the raw rows and derive helper columns
    select *, round(amount_minus_prev_amount_whole, 2) as amount_minus_prev_amount_whole_rounded
    from valid
    where merchant is not null
)
select
    id,
    txn_type,
    amount,
    is_flagged,
    merchant_upper,
    amount_whole,
    session_id,
    first_known_amount,
    prev_amount,
    case when txn_type rlike '^(DEBIT|REFUND)$' then 'CORE' else 'EXTENDED' end as txn_type_zone
from base
order by amount_whole asc nulls last, txn_type desc
