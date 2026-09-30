select we.id, we.session_id, s.started_at, c.segment from web_events we left join sessions s on we.session_id = s.id inner join customers c on c.id = s.customer_id order by started_at desc
