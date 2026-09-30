SELECT event_type, session_id, INITCAP(TRIM(page)) AS page_title, DATE_TRUNC('year', event_time) AS event_time_period FROM web_events;
