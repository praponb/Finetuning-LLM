SELECT we.duration_sec, we.session_id, s.customer_id FROM web_events we LEFT JOIN sessions s ON we.session_id = s.id WHERE we.page = '/checkout' OR s.country IN ('JP', 'UK', 'US');
