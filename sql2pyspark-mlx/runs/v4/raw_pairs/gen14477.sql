SELECT s.*
FROM sessions s
WHERE NOT EXISTS (
        SELECT 1
        FROM web_events c
        WHERE c.session_id = s.id
          AND c.event_time >= s.started_at
    )
