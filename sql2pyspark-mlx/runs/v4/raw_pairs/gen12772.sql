with filtered as (
    -- Keep recent, valid rows
    select
        id,
        account_id,
        txn_type,
        amount,
        merchant,
        is_flagged,
        hour(txn_time) as txn_time_hour
    from transactions
    where txn_time > to_date('2024-02-09')
      and account_id in (9, 18)
),
deduped as (
    -- Keep one row per key (latest first)
    select *
    from filtered
    qualify row_number() over (partition by merchant order by txn_time_hour desc, id) = 1
),
enriched as (
    -- Ranking and gap features
    select
        id,
        account_id,
        txn_type,
        amount,
        merchant,
        is_flagged,
        txn_time_hour,
        ntile(10) over w_ordered as txn_time_hour_bucket,
        row_number() over w_ordered as row_seq,
        avg(amount) over w_ordered_2 as amount_avg_3
    from deduped
    window w_ordered as (partition by txn_type order by txn_time_hour, id), w_ordered_2 as (partition by account_id, merchant order by txn_time_hour, id rows between 3 preceding and current row)
),
valid as (
    -- Basic cleansing and defaults
    select
        *,
        round(amount_avg_3 / row_seq, 2) as amount_avg_3_per_row_seq,
        coalesce(merchant, 'DIRECT') as resolved_merchant
    from enriched
    where txn_time_hour_bucket between 6 and 8
),
enriched_transactions as (
    -- Ranking and gap features
    select
        id,
        account_id,
        txn_type,
        amount,
        merchant,
        is_flagged,
        txn_time_hour,
        txn_time_hour_bucket,
        row_seq,
        amount_avg_3,
        amount_avg_3_per_row_seq,
        resolved_merchant,
        cume_dist() over (partition by txn_type order by txn_time_hour desc) as txn_time_hour_cume_dist,
        dense_rank() over (order by amount_avg_3 desc) as amount_avg_3_dense_rank
    from valid
),
latest as (
    -- Deduplicate with ROW_NUMBER
    select *
    from (
        select *, row_number() over (partition by merchant order by amount_avg_3_dense_rank desc nulls first, id) as rank_in_group
        from enriched_transactions
    ) t
    where rank_in_group = 1
)
select
    id,
    account_id,
    amount,
    is_flagged,
    txn_time_hour,
    txn_time_hour_bucket,
    amount_avg_3,
    txn_time_hour_cume_dist,
    rank_in_group
from latest
order by rank_in_group desc nulls last, amount_avg_3
