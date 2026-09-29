with ranked as (
    select *, row_number() over (partition by merchant order by txn_time desc, id) as rn
    from transactions
    qualify rn = 1
),
remaining_keys as (
    select merchant, account_id
    from ranked
    except
    select merchant, account_id
    from transactions
    where account_id in (27, 28)
),
kept_rows as (
    select s.*
    from ranked s
    inner join remaining_keys k on s.merchant = k.merchant and s.account_id = k.account_id
),
metrics as (
    select
        *,
        min(amount) over (partition by txn_type, merchant) as txn_type_min_amount,
        round(amount / sum(amount) over (partition by merchant), 4) as amount_share,
        round(amount / sum(amount) over (partition by merchant), 4) as amount_share_2
    from kept_rows
),
with_dims as (
    select
        src.*,
        d.balance,
        d.opened_date,
        d.account_type
    from metrics src
    inner join accounts d
        on src.account_id = d.id
),
with_windows as (
    select
        id,
        account_id,
        txn_type,
        amount,
        txn_time,
        merchant,
        is_flagged,
        rn,
        txn_type_min_amount,
        amount_share,
        amount_share_2,
        balance,
        opened_date,
        account_type,
        dense_rank() over w_running as amount_share_2_dense_rank,
        min(amount_share_2) over (partition by merchant) as merchant_min_amount_share_2,
        percent_rank() over (order by amount desc) as amount_percent_rank
    from with_dims
    window w_running as (order by amount_share_2 desc nulls first)
),
enriched as (
    select
        id,
        account_id,
        txn_type,
        amount,
        txn_time,
        merchant,
        is_flagged,
        rn,
        txn_type_min_amount,
        amount_share,
        amount_share_2,
        balance,
        opened_date,
        account_type,
        amount_share_2_dense_rank,
        merchant_min_amount_share_2,
        amount_percent_rank,
        rank() over (partition by account_id order by amount_share_2) as amount_share_2_rank,
        sum(amount_share) over w_ordered as amount_share_sum_90d,
        avg(txn_type_min_amount) over w_part as txn_type_min_amount_avg_90d
    from with_windows
    window w_ordered as (partition by account_id order by cast(txn_time as bigint) range between 7776000 preceding and current row), w_part as (partition by txn_type order by cast(txn_time as bigint) range between 7776000 preceding and current row)
),
cleaned as (
    select
        id,
        txn_type,
        txn_time,
        merchant,
        is_flagged,
        rn,
        txn_type_min_amount,
        amount_share,
        balance,
        account_type,
        amount_share_2_dense_rank,
        amount_percent_rank,
        amount_share_2_rank,
        amount_share_sum_90d,
        txn_type_min_amount_avg_90d,
        round(amount_share_2, 0) as amount_share_2_rounded,
        coalesce(merchant, 'DIRECT') as merchant_filled
    from enriched
    where txn_time >= to_date('2023-06-22')
      and account_type is not null
)
select *
from (
    select
        merchant,
        txn_type,
        amount_share_2_dense_rank
    from cleaned
) src
pivot (
    max(amount_share_2_dense_rank) as amount,
    count(1) as cnt
    for txn_type in ('CREDIT', 'DEBIT', 'FEE')
)
