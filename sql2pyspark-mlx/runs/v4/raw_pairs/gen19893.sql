with metrics as (
    -- Window metrics over several different partitions
    select
        *,
        rank() over (partition by session_id order by duration_sec nulls first) as duration_sec_rank,
        row_number() over (order by duration_sec desc, id) as event_seq,
        ntile(4) over (partition by page order by duration_sec, id) as duration_sec_bucket,
        sum(duration_sec) over (partition by session_id order by cast(event_time as bigint) range between 7776000 preceding and current row) as duration_sec_sum_90d
    from web_events
)
select
    id,
    session_id,
    page,
    event_time,
    duration_sec_rank,
    event_seq,
    duration_sec_bucket,
    duration_sec_sum_90d
from metrics
order by session_id desc nulls last;
