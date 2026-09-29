SELECT *, INITCAP(TRIM(page)) AS page_title, CONCAT(event_type, '/', page) AS event_type_page FROM web_events;
