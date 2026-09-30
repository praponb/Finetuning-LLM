select we.page, s.country, s.customer_id from web_events we inner join sessions s on s.id = we.session_id order by s.customer_id asc
