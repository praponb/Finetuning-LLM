SELECT *
FROM (
    SELECT event_type, COUNT(CASE WHEN event_type = 'add_to_cart' THEN 1 END) AS add_to_cart_count
    FROM web_events
    WHERE TO_DATE(event_time) < '2024-05-01'
    GROUP BY event_type
) AS s
WHERE add_to_cart_count > 5
