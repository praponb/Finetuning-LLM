with with_windows as (
    -- Window metrics over several different partitions
    select
        *,
        last_value(country) ignore nulls over (partition by customer_id order by started_at, id rows between unbounded preceding and current row) as last_known_country,
        row_number() over (partition by customer_id order by started_at desc, id) as seq_no,
        row_number() over (order by started_at, id) as row_seq
    from sessions
),
with_gaps as (
    -- Flag rows that start a new session (gap > interval)
    select *, case when lag(started_at) over (partition by customer_id order by started_at, id) is null
                 or started_at > lag(started_at) over (partition by customer_id order by started_at, id) + interval '14' day then 1 else 0 end as session_start_flag
    from with_windows
),
sessions_sessions as (
    -- Running sum of the flag = session number
    select *, sum(session_start_flag) over (partition by customer_id order by started_at, id rows between unbounded preceding and current row) as island_id
    from with_gaps
),
remaining_keys as (
    -- Set intersection on keys
    select id, device
    from sessions_sessions
    intersect all
    select id, device
    from sessions
    where browser not in ('Edge', 'Safari', 'Chrome')
),
kept_rows as (
    -- Keep rows whose keys survive the set operation
    select s.*
    from sessions_sessions s
    inner join remaining_keys k on s.id = k.id and s.device = k.device
),
metrics as (
    -- Ranking and gap features
    select
        id,
        customer_id,
        device,
        browser,
        started_at,
        country,
        last_known_country,
        seq_no,
        row_seq,
        session_start_flag,
        island_id,
        dense_rank() over (order by seq_no desc nulls first) as seq_no_dense_rank,
        rank() over (order by row_seq) as row_seq_rank,
        max(session_start_flag) over w_part as customer_id_max_session_start_flag,
        avg(seq_no) over (partition by customer_id, country order by cast(started_at as bigint) range between 2592000 preceding and current row) as seq_no_avg_30d
    from kept_rows
    window w_part as (partition by customer_id)
)
select
    s.id,
    s.customer_id,
    s.browser,
    s.started_at,
    s.last_known_country,
    s.seq_no_dense_rank,
    s.row_seq_rank,
    s.customer_id_max_session_start_flag,
    s.seq_no_avg_30d,
    (select avg(x.balance) from accounts x where x.customer_id = s.customer_id) as avg_account_balance
from metrics s
