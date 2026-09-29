with gaps as (
    -- Flag rows that start a new session (gap > interval)
    select *, case when event_time - lag(event_time) over (partition by event_type order by event_time, id) > interval '14' day then 1 else 0 end as is_new_burst
    from web_events
),
sessions_web_events as (
    -- Running sum of the flag = session number
    select
        id,
        event_type,
        page,
        event_time,
        duration_sec,
        sum(is_new_burst) over (partition by event_type order by event_time, id rows between unbounded preceding and current row) as island_id
    from gaps
),
enriched as (
    -- Running and moving aggregates
    select
        id,
        event_time,
        first_value(page) ignore nulls over (partition by page order by duration_sec, id rows between unbounded preceding and current row) as first_known_page,
        percent_rank() over (order by island_id desc) as island_id_percent_rank
    from sessions_web_events
),
scored as (
    -- Ranking and gap features
    select
        id,
        first_known_page,
        island_id_percent_rank,
        lead(island_id_percent_rank) over w_grp as next_island_id_percent_rank,
        row_number() over w_grp as row_seq,
        ntile(3) over w_part as island_id_percent_rank_bucket
    from enriched
    where hour(event_time) between 12 and 15
    window w_grp as (order by island_id_percent_rank, id), w_part as (order by island_id_percent_rank desc, id)
),
metrics as (
    -- Ranking and gap features
    select
        *,
        lead(next_island_id_percent_rank, 1, 0) over (order by island_id_percent_rank, id) as next_next_island_id_percent_rank,
        sum(island_id_percent_rank_bucket) over (order by row_seq, id rows between unbounded preceding and current row) as cumulative_island_id_percent_rank_bucket,
        row_number() over (order by island_id_percent_rank_bucket, id) as seq_no
    from scored
)
select
    id,
    island_id_percent_rank,
    next_island_id_percent_rank,
    row_seq,
    island_id_percent_rank_bucket,
    cumulative_island_id_percent_rank_bucket
from metrics
