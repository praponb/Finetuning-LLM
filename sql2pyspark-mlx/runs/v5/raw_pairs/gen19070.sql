select s.customer_id, s.started_at, s.country, we.page from sessions s left join web_events we on we.session_id = s.id where s.device in ('desktop', 'mobile', 'tablet') and we.page = '/product'
