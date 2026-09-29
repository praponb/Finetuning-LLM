select
    s.browser,
    s.customer_id,
    round(stddev(we.duration_sec), 2) as stddev_duration_sec,
    concat_ws(', ', sort_array(collect_set(we.event_type))) as event_type_values
from web_events we
inner join sessions s on we.session_id = s.id
left join customers c on c.id = s.customer_id
where s.started_at >= '2024-04-19'
group by s.browser, s.customer_id
order by 3 desc, browser
