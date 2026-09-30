with filtered as (
    -- Keep recent, valid rows
    select
        id,
        account_id,
        txn_type,
        amount,
        merchant,
        is_flagged,
        cast(txn_time as date) as txn_time_day,
        coalesce(merchant, 'NONE') as merchant_filled
    from transactions
    where merchant is not null
      and merchant != 'Lazada'
),
metrics as (
    -- Window metrics over several different partitions
    select
        *,
        ntile(10) over w_recent as amount_bucket,
        last_value(amount) ignore nulls over (order by amount, id rows between unbounded preceding and current row) as last_known_amount,
        cume_dist() over (partition by txn_type order by amount nulls first) as amount_cume_dist
    from filtered
    window w_recent as (partition by merchant order by amount desc, id)
),
metrics_transactions as (
    -- Ranking and gap features
    select
        id,
        amount,
        merchant,
        is_flagged,
        merchant_filled,
        max(amount_bucket) over (partition by account_id) as account_id_max_amount_bucket,
        round(last_known_amount / sum(last_known_amount) over (partition by txn_type), 4) as last_known_amount_share,
        first_value(amount, true) over (order by amount, id rows between unbounded preceding and current row) as first_known_amount,
        dense_rank() over (partition by account_id order by amount_cume_dist desc nulls last) as amount_cume_dist_dense_rank
    from metrics
),
with_windows as (
    -- Ranking and gap features
    select
        id,
        account_id_max_amount_bucket,
        last_known_amount_share,
        first_known_amount,
        amount_cume_dist_dense_rank,
        lead(last_known_amount_share) over (partition by merchant order by amount_cume_dist_dense_rank, id) as next_last_known_amount_share,
        lag(amount) over (partition by merchant order by amount_cume_dist_dense_rank, id) as prev_amount,
        lead(amount) over (partition by merchant order by amount_cume_dist_dense_rank, id) as next_amount,
        row_number() over (partition by merchant order by amount_cume_dist_dense_rank, id) as seq_no,
        sum(amount) over (partition by merchant order by amount_cume_dist_dense_rank, id rows between unbounded preceding and current row) as running_amount
    from metrics_transactions
    where amount between 1100 and 1200
),
enriched as (
    -- Window metrics over several different partitions
    select
        *,
        avg(running_amount) over w_running as running_amount_avg_5_prior,
        first_value(account_id_max_amount_bucket, true) over (order by seq_no, id rows between unbounded preceding and current row) as first_known_account_id_max_amount_bucket,
        lead(next_amount, 1) over (order by next_last_known_amount_share, id) as next_next_amount,
        ntile(10) over w_running_2 as amount_cume_dist_dense_rank_bucket,
        row_number() over w_recent as event_seq
    from with_windows
    window w_running as (order by prev_amount, id rows between 5 preceding and 1 preceding), w_running_2 as (order by amount_cume_dist_dense_rank desc, id), w_recent as (order by next_amount, id)
),
stats as (
    -- Aggregate with conditional metrics
    select
        seq_no,
        count(*) as cnt,
        max(prev_amount) as max_prev_amount,
        approx_percentile(seq_no, array(0.25, 0.5, 0.75)) as seq_no_quartiles,
        count(*) as event_count,
        approx_percentile(first_known_amount, array(0.25, 0.5, 0.75)) as first_known_amount_quartiles
    from enriched
    group by seq_no
)
select
    seq_no,
    cnt,
    seq_no_quartiles,
    event_count
from stats
order by event_count desc nulls first, seq_no
