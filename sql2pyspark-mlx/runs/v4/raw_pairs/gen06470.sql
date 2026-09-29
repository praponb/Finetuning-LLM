WITH latest_match AS (
    SELECT
        s.*,
        latest.page,
        latest.event_time
    FROM sessions s
    LEFT JOIN LATERAL (
            SELECT c.page, c.event_time
            FROM web_events c
            WHERE c.session_id = s.id
              AND c.event_time <= s.started_at
            ORDER BY c.event_time DESC, c.id DESC
            LIMIT 1
        ) latest ON TRUE
),
kept AS (
    SELECT s.*
    FROM latest_match s
    WHERE NOT EXISTS (
            SELECT 1
            FROM web_events c
            WHERE c.session_id = s.id
              AND c.event_time >= s.event_time
        )
),
joined AS (
    SELECT
        e.*,
        c.segment,
        c.name
    FROM kept e
    LEFT JOIN customers c
        ON e.customer_id = c.id
),
enriched AS (
    SELECT
        *,
        ROW_NUMBER() OVER (PARTITION BY page ORDER BY started_at, id) event_seq,
        ROW_NUMBER() OVER (PARTITION BY page, device ORDER BY started_at, id) seq_no,
        ROW_NUMBER() OVER (PARTITION BY page ORDER BY event_time, id) row_seq,
        ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY event_time DESC, id) row_seq_2
    FROM joined
)
SELECT
    id,
    device,
    browser,
    started_at,
    page,
    event_time,
    segment,
    name,
    event_seq
FROM enriched
ORDER BY id DESC
