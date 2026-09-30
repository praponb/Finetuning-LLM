with enriched as (
    select
        *,
        first_value(duration_sec) ignore nulls over (partition by event_type order by event_time, id rows between unbounded preceding and current row) as first_known_duration_sec,
        count(duration_sec) over (partition by session_id) as session_id_count_duration_sec,
        last_value(page) ignore nulls over (partition by page, session_id order by event_time, id rows between unbounded preceding and current row) as last_known_page
    from web_events
)
select
    event_type,
    session_id,
    approx_percentile(first_known_duration_sec, 0.5) as first_known_duration_sec_p50,
    min(event_time) as first_event_time,
    approx_percentile(session_id_count_duration_sec, array(0.25, 0.5, 0.75)) as session_id_count_duration_sec_quartiles,
    grouping_id() as grouping_id
from enriched
group by cube (event_type, session_id)
order by session_id nulls last;
