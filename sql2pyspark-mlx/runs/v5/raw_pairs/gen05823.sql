select s.id, s.country, t.total_duration_sec
from sessions s
inner join (select session_id, sum(duration_sec) total_duration_sec
    from web_events
    where hour(event_time) between 6 and 10
    group by session_id) t on s.id = t.session_id
where t.total_duration_sec > 2600
order by id desc;
