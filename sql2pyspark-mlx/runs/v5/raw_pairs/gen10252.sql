select s.browser, s.customer_id, we.session_id from sessions s left join web_events we on we.session_id = s.id where s.country = 'TH' order by s.browser;
