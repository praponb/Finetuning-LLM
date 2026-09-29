select
    s.browser,
    count(we.id) as web_event_count,
    sum(case when we.page = '/search' then we.duration_sec else 0 end) as search_duration_sec,
    round(stddev(we.duration_sec), 2) as stddev_duration_sec
from sessions s
left join web_events we on s.id = we.session_id
where s.customer_id is not null
group by s.browser;
