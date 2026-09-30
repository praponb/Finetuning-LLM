WITH base AS (
    SELECT
        id,
        customer_id,
        browser,
        started_at,
        country,
        CASE WHEN device = 'mobile' THEN 1 ELSE 0 END AS is_mobile,
        TRIM(device) AS clean_device
    FROM sessions
    WHERE country IS NOT NULL
      AND browser IS DISTINCT FROM 'Firefox'
)
SELECT s.*
FROM base s
WHERE NOT EXISTS (
        SELECT 1
        FROM web_events c
        WHERE c.session_id = s.id
          AND c.event_time < s.started_at
    );
