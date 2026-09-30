with cleaned as (
    select
        id,
        session_id,
        event_type,
        page,
        event_time,
        duration_sec,
        concat(page, '/', event_type) as page_event_type,
        coalesce(page, 'UNKNOWN') as resolved_page
    from web_events
    where page is not null
      and event_time >= timestamp '2023-05-22 00:00:00'
),
flagged as (
    select *, case when event_time - lag(event_time) over (partition by page order by event_time, id) > interval 7 days then 1 else 0 end as is_new_burst
    from cleaned
),
islands as (
    select *, sum(is_new_burst) over (partition by page order by event_time, id rows between unbounded preceding and current row) as session_no
    from flagged
),
enriched as (
    select
        *,
        ntile(10) over (partition by event_type order by is_new_burst, id) as is_new_burst_bucket,
        row_number() over (partition by page order by is_new_burst, id) as seq_no,
        row_number() over (partition by page, event_type order by event_time desc, id) as row_seq
    from islands
),
with_windows as (
    select
        *,
        ntile(4) over w_ordered as seq_no_bucket,
        first_value(row_seq) ignore nulls over (partition by session_id order by session_no, id rows between unbounded preceding and current row) as first_known_row_seq,
        first_value(duration_sec) ignore nulls over w_recent as first_known_duration_sec,
        sum(is_new_burst_bucket) over w_recent_2 as running_is_new_burst_bucket
    from enriched
    window w_ordered as (partition by event_type, page order by seq_no desc, id), w_recent as (partition by page order by duration_sec, id rows between unbounded preceding and current row), w_recent_2 as (order by duration_sec, id rows between unbounded preceding and current row)
)
select
    event_type,
    case when first_known_row_seq >= 7 then 'PREMIUM' else 'STANDARD' end as first_known_row_seq_tier,
    count(distinct session_id) as distinct_session_ids,
    count(*) filter (where page = '/home') as home_rows,
    round(avg(is_new_burst_bucket), 2) as avg_is_new_burst_bucket,
    round(sum(seq_no_bucket) / nullif(count(case when page = '/cart' then 1 end), 0), 2) as seq_no_bucket_per_cart,
    max(is_new_burst) filter (where event_type = 'page_view') as page_view_max_is_new_burst
from with_windows
group by grouping sets ((event_type, case when first_known_row_seq >= 7 then 'PREMIUM' else 'STANDARD' end), (event_type))
order by home_rows desc;
