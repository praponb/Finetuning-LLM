select we.id, we.page from web_events we where we.session_id in (select id from sessions where browser in ('Firefox', 'Safari'));
