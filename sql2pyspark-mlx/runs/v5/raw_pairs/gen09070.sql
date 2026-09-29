WITH with_latest AS (
    SELECT s.*, latest.session_browser
    FROM customers s
    JOIN LATERAL (
            SELECT c.browser AS session_browser
            FROM sessions c
            WHERE c.customer_id = s.id
            ORDER BY c.started_at DESC, c.id DESC
            LIMIT 1
        ) latest ON TRUE
),
filtered AS (
    SELECT
        *,
        TRIM(email) AS clean_email,
        COALESCE(city, 'N/A') AS resolved_city
    FROM with_latest
    WHERE session_browser IS NOT NULL
      AND signup_date >= DATE '2023-02-28'
),
not_flagged AS (
    SELECT s.*
    FROM filtered s
    WHERE NOT EXISTS (
            SELECT 1
            FROM orders c
            WHERE c.customer_id = s.id
              AND c.store_id = 33
        )
),
metrics AS (
    SELECT
        *,
        MAX(age) OVER (PARTITION BY session_browser) AS session_browser_max_age,
        ROUND(age / SUM(age) OVER (PARTITION BY segment), 4) AS age_share
    FROM not_flagged
),
valid AS (
    SELECT
        *,
        CASE WHEN city = 'Bangkok' THEN 1 ELSE 0 END AS is_bangkok,
        COALESCE(session_browser, 'NONE') AS session_browser_filled
    FROM metrics
    WHERE city = 'Chiang Mai'
      AND segment IS NOT NULL
),
base AS (
    SELECT
        *,
        TRIM(email) AS clean_email_2,
        CASE WHEN city IN ('Bangkok', 'London') THEN 'Group A' ELSE 'Other' END AS city_group
    FROM valid
    WHERE signup_date >= DATE '2023-03-30'
      AND is_bangkok < 46
)
SELECT
    CASE WHEN is_bangkok >= 70 THEN 'PREMIUM' ELSE 'STANDARD' END AS is_bangkok_tier,
    CASE WHEN age >= 61 THEN 'HIGH' ELSE 'LOW' END AS age_tier,
    COUNT(DISTINCT city) AS distinct_cities,
    COUNT(DISTINCT id) AS distinct_ids,
    ROUND(AVG(session_browser_max_age), 2) AS avg_session_browser_max_age,
    SORT_ARRAY(COLLECT_SET(session_browser)) AS session_browser_list,
    SORT_ARRAY(COLLECT_SET(session_browser)) AS session_browser_list_2,
    SIZE(COLLECT_SET(segment)) AS num_segments
FROM base
GROUP BY is_bangkok_tier, age_tier
HAVING num_segments >= 1
ORDER BY num_segments DESC;
