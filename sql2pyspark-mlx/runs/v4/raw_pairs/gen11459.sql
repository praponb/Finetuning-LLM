with with_windows as (
    select
        *,
        ntile(4) over w_hist as duration_sec_bucket,
        lag(duration_sec, 2, 0) over (partition by session_id order by duration_sec, id) as prev_duration_sec
    from web_events
    window w_hist as (partition by session_id order by duration_sec desc, id)
)
select
    event_type,
    case when duration_sec_bucket >= 8 then 'HIGH' else 'LOW' end as duration_sec_bucket_tier,
    page,
    bit_or(duration_sec) as duration_sec_bit_or,
    sum(case when page = '/search' then prev_duration_sec else 0 end) as search_prev_duration_sec,
    count(*) filter (where event_type = 'purchase') as purchase_rows,
    count(*) as num_rows,
    (unix_timestamp(max(event_time)) - unix_timestamp(min(event_time))) / 60 as duration_min,
    round(avg(duration_sec_bucket), 2) as avg_duration_sec_bucket,
    grouping(event_type) as gid
from with_windows
group by cube (event_type, case when duration_sec_bucket >= 8 then 'HIGH' else 'LOW' end, page)
having count(*) < 1
order by purchase_rows;
