SELECT
    id,
    page,
    CASE WHEN page = '/cart' THEN 1 ELSE 0 END AS is_cart,
    LOWER(event_type) AS event_type_lower
FROM web_events
WHERE session_id IN (4, 11, 12, 35);
