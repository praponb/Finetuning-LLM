with scored as (
    -- Window metrics over several different partitions
    select
        *,
        ntile(3) over (partition by event_type order by duration_sec desc, id) as duration_sec_bucket,
        ntile(3) over (partition by page order by duration_sec desc, id) as duration_sec_bucket_2
    from web_events
),
scored_web_events as (
    -- Window metrics over several different partitions
    select
        *,
        lead(duration_sec, 1, 0) over w_ordered as next_duration_sec,
        last_value(duration_sec_bucket) ignore nulls over (partition by event_type order by event_time, id rows between unbounded preceding and current row) as last_known_duration_sec_bucket,
        lag(duration_sec_bucket_2, 1) over (partition by session_id, event_type order by duration_sec_bucket, id) as prev_duration_sec_bucket_2,
        sum(duration_sec) over w_grp as event_type_sum_duration_sec
    from scored
    window w_ordered as (partition by page, event_type order by duration_sec, id), w_grp as (partition by event_type)
),
latest as (
    -- Deduplicate with ROW_NUMBER
    select *
    from (
        select *, rank() over (partition by session_id, event_type order by event_type_sum_duration_sec nulls first) as rn
        from scored_web_events
    ) t
    where rn = 1
),
filtered as (
    -- Keep recent, valid rows
    select
        id,
        session_id,
        event_type,
        page,
        duration_sec,
        duration_sec_bucket,
        next_duration_sec,
        last_known_duration_sec_bucket,
        prev_duration_sec_bucket_2,
        rn,
        case when prev_duration_sec_bucket_2 >= 7 then 'High' when prev_duration_sec_bucket_2 >= 4 then 'Medium' else 'Low' end as prev_duration_sec_bucket_2_band,
        coalesce(page, 'UNKNOWN') as page_filled
    from latest
    where event_time >= to_timestamp('2024-02-13 14:00:00')
)
-- Ranking and gap features
select
    id,
    session_id,
    event_type,
    page,
    duration_sec,
    duration_sec_bucket,
    next_duration_sec,
    last_known_duration_sec_bucket,
    prev_duration_sec_bucket_2,
    rn,
    prev_duration_sec_bucket_2_band,
    page_filled,
    avg(next_duration_sec) over (partition by event_type order by rn, id rows between 2 preceding and 1 preceding) as next_duration_sec_avg_2_prior,
    row_number() over (partition by session_id, event_type order by last_known_duration_sec_bucket desc, id) as event_seq
from filtered
