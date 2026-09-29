WITH valid AS (
    SELECT
        *,
        DATE_FORMAT(started_at, 'yyyy-MM-dd') AS started_at_label,
        COALESCE(country, 'OTHER') AS country_clean
    FROM sessions
    WHERE started_at > DATE '2023-06-18'
      AND device = 'mobile'
),
enriched AS (
    SELECT
        src.id,
        src.customer_id,
        src.browser,
        src.started_at_label,
        src.country_clean,
        p.name,
        p.country AS customer_country
    FROM valid src
    LEFT JOIN customers p
        ON src.customer_id = p.id
),
kept AS (
    SELECT s.*
    FROM enriched s
    WHERE NOT EXISTS (
            SELECT 1
            FROM web_events c
            WHERE c.session_id = s.id
              AND c.event_type IN ('search', 'page_view', 'purchase')
        )
),
not_flagged AS (
    SELECT s.*
    FROM kept s
    WHERE EXISTS (
            SELECT 1
            FROM web_events c
            WHERE c.session_id = s.id
              AND c.session_id IN (20, 36)
        )
),
with_last AS (
    SELECT
        s.id,
        s.browser,
        s.started_at_label,
        s.name,
        s.customer_country,
        top1.page,
        top1.duration_sec
    FROM not_flagged s
    JOIN LATERAL (
            SELECT c.page, c.duration_sec
            FROM web_events c
            WHERE c.session_id = s.id
            ORDER BY c.duration_sec DESC, c.id DESC
            LIMIT 1
        ) top1 ON TRUE
)
SELECT
    *,
    LEAD(duration_sec) OVER (PARTITION BY customer_country ORDER BY duration_sec, id) AS next_duration_sec,
    LAG(duration_sec) OVER (PARTITION BY customer_country ORDER BY duration_sec, id) AS prev_duration_sec,
    ROW_NUMBER() OVER (PARTITION BY customer_country ORDER BY duration_sec, id) AS event_seq,
    NTILE(3) OVER (PARTITION BY browser ORDER BY duration_sec DESC, id) AS duration_sec_bucket
FROM with_last
