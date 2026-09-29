SELECT s.*
FROM sessions s
WHERE NOT EXISTS (
        SELECT 1
        FROM web_events ch
        WHERE ch.session_id = s.id
          AND ch.event_time < '2025-03-12'
    )
