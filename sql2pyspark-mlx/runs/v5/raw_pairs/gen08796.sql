SELECT
    id,
    page,
    CONCAT(event_type, ' ', page) AS event_type_page,
    TO_DATE(event_time) AS event_time_date
FROM web_events
WHERE page IN ('/home', '/cart', '/checkout')
