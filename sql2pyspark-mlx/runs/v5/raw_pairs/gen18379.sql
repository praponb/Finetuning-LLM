SELECT *
FROM (
    SELECT
        page,
        event_type,
        duration_sec
    FROM web_events
) src
PIVOT (
    MAX(duration_sec) AS total
    FOR event_type IN ('add_to_cart', 'search', 'click')
);
