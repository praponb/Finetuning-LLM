with cleaned as (
    -- Filter the raw rows and derive helper columns
    select
        *,
        initcap(trim(page)) as page_title,
        coalesce(page, 'DIRECT') as page_clean
    from web_events
    where hour(event_time) between 10 and 13
),
with_windows as (
    -- Window metrics over several different partitions
    select
        *,
        avg(duration_sec) over (order by event_time, id rows between 6 preceding and current row) as duration_sec_avg_6,
        count(duration_sec) over (order by cast(event_time as bigint) range between 2592000 preceding and current row) as duration_sec_count_30d,
        avg(duration_sec) over w_part as duration_sec_avg_6_2,
        dense_rank() over (partition by session_id order by duration_sec nulls last) as duration_sec_dense_rank
    from cleaned
    window w_part as (partition by event_type order by duration_sec, id rows between 6 preceding and current row)
),
enriched as (
    -- Attach sessions attributes
    select a.*, b.device
    from with_windows a
    left join sessions b
        on a.session_id = b.id
)
-- Window metrics over several different partitions
select
    id,
    session_id,
    event_type,
    page,
    event_time,
    duration_sec,
    page_title,
    page_clean,
    duration_sec_avg_6,
    duration_sec_count_30d,
    duration_sec_avg_6_2,
    duration_sec_dense_rank,
    device,
    lag(duration_sec_avg_6, 1) over (order by duration_sec_dense_rank, id) as prev_duration_sec_avg_6,
    sum(duration_sec_dense_rank) over (order by cast(event_time as bigint) range between 2592000 preceding and current row) as duration_sec_dense_rank_sum_30d,
    row_number() over (partition by page, device order by duration_sec_count_30d desc, id) as row_seq
from enriched
