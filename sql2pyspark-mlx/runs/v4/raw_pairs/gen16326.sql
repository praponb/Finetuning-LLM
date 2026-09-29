select
    session_id,
    percentile_approx(duration_sec, 0.5) as duration_sec_p50,
    min(event_time) as started_at,
    count(distinct session_id) as distinct_session_ids
from web_events
group by rollup (session_id)
order by duration_sec_p50
