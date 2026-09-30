WITH cleaned AS (
    -- Keep recent, valid rows
    SELECT *, COALESCE(country, 'N/A') country_filled
    FROM sessions
    WHERE started_at > TIMESTAMP '2023-03-08 10:00:00'
      AND browser IS NOT NULL
),
eligible AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM cleaned s
    WHERE NOT EXISTS (
            SELECT 1
            FROM web_events c
            WHERE c.session_id = s.id
              AND c.event_time <= s.started_at
        )
),
enriched AS (
    -- Ranking and gap features
    SELECT
        *,
        LAST_VALUE(country) IGNORE NULLS OVER (PARTITION BY browser, country ORDER BY started_at ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) last_known_country,
        FIRST_VALUE(country) IGNORE NULLS OVER w_grp first_known_country,
        FIRST_VALUE(country) IGNORE NULLS OVER (PARTITION BY device, customer_id ORDER BY started_at ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) first_known_country_2,
        FIRST_VALUE(browser, TRUE) OVER w_grp first_known_browser
    FROM eligible
    WINDOW w_grp AS (ORDER BY started_at ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
),
enriched_sessions AS (
    -- Ranking and gap features
    SELECT
        id,
        customer_id,
        device,
        browser,
        started_at,
        country,
        country_filled,
        last_known_country,
        first_known_country,
        first_known_country_2,
        first_known_browser,
        ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY started_at DESC, id ASC) event_seq,
        ROW_NUMBER() OVER (PARTITION BY device ORDER BY started_at ASC, id ASC) row_seq,
        ROW_NUMBER() OVER (PARTITION BY browser ORDER BY started_at DESC, id ASC) event_seq_2
    FROM enriched
)
SELECT
    id,
    customer_id,
    device,
    started_at,
    country,
    country_filled,
    first_known_country,
    first_known_browser,
    event_seq,
    CASE WHEN country RLIKE '^(JP|UK)$' THEN 'PRIORITY' WHEN country RLIKE '^(TH|US)$' THEN 'OTHER_ZONE' ELSE 'CORE' END AS country_zone
FROM enriched_sessions
ORDER BY id DESC, country ASC;
