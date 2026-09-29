with enriched as (
    -- Window metrics over several different partitions
    select
        id,
        session_id,
        event_type,
        lead(duration_sec) over (partition by session_id order by duration_sec, id) next_duration_sec,
        lag(duration_sec) over (partition by session_id order by duration_sec, id) prev_duration_sec,
        row_number() over (partition by session_id order by duration_sec, id) seq_no
    from web_events
    where event_time < '2023-03-01'
),
valid as (
    -- Basic cleansing and defaults
    select
        id,
        session_id,
        prev_duration_sec,
        seq_no,
        next_duration_sec * prev_duration_sec next_duration_sec_times_prev_duration_sec
    from enriched
    where session_id in (4, 13)
),
ranked as (
    -- QUALIFY-style window filter
    select *
    from (
        select *, row_number() over (partition by session_id order by next_duration_sec_times_prev_duration_sec, id) as rank_in_group
        from valid
    ) x
    where rank_in_group = 1
),
ranked_web_events as (
    -- Deduplicate with ROW_NUMBER
    select *, row_number() over (partition by session_id order by rank_in_group desc, id) as latest_rank
    from ranked
    qualify latest_rank = 1
)
-- Grouped metrics
select
    seq_no,
    min(seq_no) as min_seq_no,
    count(distinct id) as distinct_ids,
    percentile_approx(prev_duration_sec, 0.95) as prev_duration_sec_p95,
    count(distinct id) as distinct_ids_2,
    avg(seq_no) as avg_seq_no,
    count(distinct session_id) as distinct_session_ids
from ranked_web_events
group by rollup (seq_no)
having distinct_ids_2 > 1;
