with cleaned as (
    -- Filter the raw rows and derive helper columns
    select
        id,
        txn_type,
        amount,
        merchant,
        is_flagged,
        date_trunc('YEAR', txn_time) as txn_time_period,
        coalesce(merchant, 'N/A') as merchant_filled
    from transactions
    where merchant is not null
),
latest as (
    -- QUALIFY-style window filter
    select *
    from (
        select *, row_number() over (partition by merchant order by amount nulls last, id) as latest_rank
        from cleaned
    ) ranked
    where latest_rank = 1
),
scored as (
    -- Running and moving aggregates
    select
        id,
        amount,
        merchant,
        txn_time_period,
        merchant_filled,
        latest_rank,
        sum(amount) over (partition by txn_type, merchant order by txn_time_period, id rows between unbounded preceding and current row) as cumulative_amount,
        rank() over (partition by merchant order by amount desc nulls last) as amount_rank,
        sum(amount) over (partition by txn_type order by cast(txn_time_period as bigint) range between 604800 preceding and current row) as amount_sum_7d
    from latest
    where txn_time_period >= '2023-10-01'
),
with_windows as (
    -- Window metrics over several different partitions
    select
        *,
        lead(amount_rank) over (partition by merchant order by amount_rank, id) as next_amount_rank,
        lag(amount_rank) over (partition by merchant order by amount_rank, id) as prev_amount_rank,
        lead(amount) over (partition by merchant order by amount_rank, id) as next_amount,
        row_number() over (partition by merchant order by amount_rank, id) as row_seq
    from scored
),
enriched as (
    -- Ranking and gap features
    select
        id,
        amount,
        merchant,
        txn_time_period,
        merchant_filled,
        amount_rank,
        next_amount_rank,
        next_amount,
        row_seq,
        max(next_amount) over (partition by merchant) as merchant_max_next_amount,
        stddev_samp(prev_amount_rank) over (partition by merchant) as merchant_stddev_samp_prev_amount_rank,
        last_value(merchant) ignore nulls over (partition by merchant order by amount_sum_7d, id rows between unbounded preceding and current row) as last_known_merchant
    from with_windows
),
gaps as (
    -- Flag rows that start a new session (gap > interval)
    select *, case when txn_time_period - lag(txn_time_period) over (partition by merchant order by txn_time_period, id) > interval 14 days then 1 else 0 end as is_new_session
    from enriched
),
sessions_transactions as (
    -- Running sum of the flag = session number
    select
        id,
        amount,
        merchant,
        txn_time_period,
        amount_rank,
        next_amount_rank,
        row_seq,
        merchant_stddev_samp_prev_amount_rank,
        last_known_merchant,
        sum(is_new_session) over (partition by merchant order by txn_time_period, id rows between unbounded preceding and current row) as burst_id
    from gaps
)
-- Window metrics over several different partitions
select
    *,
    row_number() over (partition by merchant order by amount, id) as row_seq_2,
    avg(merchant_stddev_samp_prev_amount_rank) over (order by txn_time_period, id rows between 2 preceding and 1 preceding) as merchant_stddev_samp_prev_amount_rank_avg_2_prior,
    ntile(10) over (partition by merchant order by amount_rank, id) as amount_rank_bucket,
    sum(amount_rank) over w_grp as running_amount_rank,
    ntile(3) over w_ordered as burst_id_bucket
from sessions_transactions
window w_grp as (partition by merchant order by merchant_stddev_samp_prev_amount_rank, id rows between unbounded preceding and current row), w_ordered as (partition by merchant order by burst_id desc, id)
