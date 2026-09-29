with gaps as (
    select *, case when event_time - lag(event_time) over (partition by page order by event_time, id) > interval '30' day then 1 else 0 end as session_start_flag
    from web_events
),
sessions_web_events as (
    select
        id,
        session_id,
        event_type,
        page,
        event_time,
        duration_sec,
        sum(session_start_flag) over (partition by page order by event_time, id rows between unbounded preceding and current row) as burst_id
    from gaps
)
select
    date_trunc('week', event_time) as week_start,
    approx_percentile(burst_id, 0.95) as burst_id_p95,
    count(*) filter (where event_type = 'purchase') as purchase_rows,
    sum(burst_id) filter (where event_type = 'click') as click_total_burst_id,
    count(case when page = '/search' then 1 end) as search_count,
    size(collect_set(page)) as num_pages,
    round(sum(duration_sec) / nullif(count(case when page = '/product' then 1 end), 0), 2) as duration_sec_per_product
from sessions_web_events
group by rollup (date_trunc('week', event_time))
