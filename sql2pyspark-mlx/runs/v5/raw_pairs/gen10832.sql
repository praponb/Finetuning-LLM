with with_gaps as (
    select *, case when lag(event_time) over (partition by session_id order by event_time, id) is null
                 or event_time > lag(event_time) over (partition by session_id order by event_time, id) + interval '60' day then 1 else 0 end as session_start_flag
    from web_events
),
islands as (
    select
        id,
        session_id,
        event_type,
        page,
        event_time,
        duration_sec,
        sum(session_start_flag) over (partition by session_id order by event_time, id rows between unbounded preceding and current row) as session_no
    from with_gaps
)
select
    page,
    date_trunc('month', event_time) as activity_month,
    count_if(page = '/checkout') as checkout_events,
    percentile_approx(duration_sec, 0.9) as duration_sec_p90,
    round(avg(session_no), 2) as avg_session_no,
    count_if(page = '/cart') as cart_events,
    count_if(event_type = 'purchase') as purchase_events
from islands
group by page, date_trunc('month', event_time);
