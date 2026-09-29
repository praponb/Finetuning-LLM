with with_windows as (
    -- Running and moving aggregates
    select
        id,
        order_id,
        method,
        amount,
        paid_at,
        lag(amount) over w_part as prev_amount,
        lead(amount) over w_part as next_amount,
        row_number() over w_part as seq_no
    from payments
    where to_date(paid_at) >= '2023-10-01'
    window w_part as (partition by order_id, status order by paid_at, id)
),
metrics as (
    -- Ranking and gap features
    select
        *,
        avg(next_amount) over (partition by method order by next_amount, id rows between 3 preceding and current row) as next_amount_avg_3,
        row_number() over (partition by order_id order by prev_amount desc, id) as row_seq,
        ntile(10) over (partition by order_id order by seq_no, id) as seq_no_bucket
    from with_windows
),
flagged as (
    -- Flag rows that start a new session (gap > interval)
    select *, case when lag(paid_at) over (partition by method order by paid_at, id) is null
                 or paid_at > lag(paid_at) over (partition by method order by paid_at, id) + interval '14' day then 1 else 0 end as session_start_flag
    from metrics
),
islands as (
    -- Running sum of the flag = session number
    select *, sum(session_start_flag) over (partition by method order by paid_at, id rows between unbounded preceding and current row) as island_id
    from flagged
),
with_windows_payments as (
    -- Window metrics over several different partitions
    select
        *,
        lead(next_amount) over (partition by method, order_id order by next_amount_avg_3, id) as next_next_amount,
        lag(next_amount) over (partition by method, order_id order by next_amount_avg_3, id) as prev_next_amount,
        lead(next_amount_avg_3) over (partition by method, order_id order by next_amount_avg_3, id) as next_next_amount_avg_3,
        row_number() over (partition by method, order_id order by next_amount_avg_3, id) as seq_no_2,
        ntile(4) over (partition by order_id, method order by next_amount, id) as next_amount_bucket
    from islands
),
with_windows_payments_2 as (
    -- Ranking and gap features
    select
        id,
        prev_amount,
        next_amount,
        next_amount_avg_3,
        row_seq,
        session_start_flag,
        island_id,
        next_next_amount,
        seq_no_2,
        next_amount_bucket,
        lag(next_next_amount_avg_3) over (partition by method order by next_amount, id) as prev_next_next_amount_avg_3,
        lag(next_amount) over (partition by method order by next_amount, id) as prev_next_amount_2,
        row_number() over (partition by method order by next_amount, id) as row_seq_2,
        avg(session_start_flag) over (partition by order_id order by seq_no, id rows between 2 preceding and 1 preceding) as session_start_flag_avg_2_prior
    from with_windows_payments
)
-- Aggregate with conditional metrics
select
    case when prev_next_next_amount_avg_3 >= 1351 then 'HIGH' else 'LOW' end as prev_next_next_amount_avg_3_tier,
    case when prev_next_amount_2 >= 1430 then 'LARGE' else 'SMALL' end as prev_next_amount_2_tier,
    approx_percentile(next_amount, 0.95) as next_amount_p95,
    round(avg(prev_next_next_amount_avg_3), 2) as avg_prev_next_next_amount_avg_3,
    approx_percentile(row_seq, array(0.25, 0.5, 0.75)) as row_seq_quartiles,
    sum(row_seq) as total_row_seq
from with_windows_payments_2
group by rollup (case when prev_next_next_amount_avg_3 >= 1351 then 'HIGH' else 'LOW' end, case when prev_next_amount_2 >= 1430 then 'LARGE' else 'SMALL' end)
having round(avg(prev_next_next_amount_avg_3), 2) >= 0
