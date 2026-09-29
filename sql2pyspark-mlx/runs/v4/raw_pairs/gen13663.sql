with filtered as (
    -- Filter the raw rows and derive helper columns
    select
        id,
        txn_type,
        amount,
        txn_time,
        cast(is_flagged as int) as is_flagged_flag
    from transactions
    where txn_time > timestamp '2023-10-15 20:00:00'
      and merchant is not null
      and is_flagged = true
),
flagged as (
    -- Flag rows that start a new session (gap > interval)
    select *, case when lag(txn_time) over (partition by txn_type order by txn_time, id) is null
                 or txn_time > lag(txn_time) over (partition by txn_type order by txn_time, id) + interval 60 days then 1 else 0 end as is_new_burst
    from filtered
),
sessionized as (
    -- Running sum of the flag = session number
    select *, sum(is_new_burst) over (partition by txn_type order by txn_time, id rows between unbounded preceding and current row) as session_id
    from flagged
),
with_windows as (
    -- Running and moving aggregates
    select
        *,
        sum(session_id) over (order by amount, id rows between unbounded preceding and current row) as cumulative_session_id,
        stddev_samp(is_flagged_flag) over (partition by txn_type) as txn_type_stddev_samp_is_flagged_flag,
        last_value(is_flagged_flag) ignore nulls over (partition by txn_type order by session_id, id rows between unbounded preceding and current row) as last_known_is_flagged_flag
    from sessionized
),
valid as (
    -- Basic cleansing and defaults
    select
        id,
        txn_type,
        amount,
        is_flagged_flag,
        is_new_burst,
        session_id,
        cumulative_session_id,
        txn_type_stddev_samp_is_flagged_flag,
        substring(txn_type, 1, 2) as txn_type_prefix,
        datediff(current_date(), txn_time) as days_since_txn_time
    from with_windows
    where txn_type_stddev_samp_is_flagged_flag >= 39
),
scored as (
    -- Ranking and gap features
    select
        *,
        row_number() over (partition by txn_type order by cumulative_session_id desc, id) as row_seq,
        row_number() over (partition by txn_type order by is_new_burst, id) as seq_no,
        ntile(3) over (partition by txn_type order by is_new_burst desc, id) as is_new_burst_bucket
    from valid
),
cleaned as (
    -- Keep recent, valid rows
    select
        *,
        round(txn_type_stddev_samp_is_flagged_flag * 1.15, 2) as txn_type_stddev_samp_is_flagged_flag_scaled,
        trim(txn_type) as clean_txn_type
    from scored
    where is_new_burst_bucket != 4
),
stats as (
    -- Multi-level aggregation
    select
        case when session_id >= 6 then 'LARGE' else 'SMALL' end as session_id_tier,
        seq_no,
        count(distinct id) as distinct_ids,
        approx_percentile(cumulative_session_id, array(0.25, 0.5, 0.75)) as cumulative_session_id_quartiles,
        sort_array(collect_set(txn_type)) as txn_type_list,
        round(avg(seq_no), 2) as avg_seq_no,
        count(case when txn_type = 'CREDIT' then 1 end) as credit_count,
        percentile_approx(txn_type_stddev_samp_is_flagged_flag_scaled, 0.95) as txn_type_stddev_samp_is_flagged_flag_scaled_p95
    from cleaned
    group by 1, 2
    having round(avg(seq_no), 2) >= 0
)
select
    session_id_tier,
    seq_no,
    credit_count,
    txn_type_stddev_samp_is_flagged_flag_scaled_p95
from stats
order by session_id_tier nulls last, txn_type_stddev_samp_is_flagged_flag_scaled_p95
