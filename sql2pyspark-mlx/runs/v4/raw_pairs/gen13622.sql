SELECT s.*
FROM sessions s
WHERE EXISTS (
        SELECT 1
        FROM web_events x
        WHERE x.session_id = s.id
          AND x.page IN ('/checkout', '/search')
    );
