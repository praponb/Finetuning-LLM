select s.id, count(we.id) as web_event_count, round((100 * sum(case when we.page = '/checkout' then 1 else 0 end)) / count(*), 1) as pct_checkout
from sessions s
left join web_events we on s.id = we.session_id
where we.session_id in (8, 22, 32, 39)
group by s.id
order by 3 desc, id
limit 3
