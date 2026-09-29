WITH cleaned AS (
    -- Keep recent, valid rows
    SELECT
        id,
        name,
        country,
        city,
        is_active,
        credit_limit,
        age,
        CASE WHEN is_active THEN 'Yes' ELSE 'No' END AS is_active_label
    FROM customers
    WHERE city IS NOT NULL
),
with_last AS (
    -- Latest matching session per row (LATERAL ... LIMIT 1)
    SELECT
        s.id,
        s.credit_limit,
        s.age,
        x.session_browser
    FROM cleaned s
    JOIN LATERAL (
            SELECT c.browser AS session_browser
            FROM sessions c
            WHERE c.customer_id = s.id
            ORDER BY c.started_at DESC, c.id DESC
            LIMIT 1
        ) x ON TRUE
),
eligible AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM with_last s
    WHERE NOT EXISTS (
            SELECT 1
            FROM sessions x
            WHERE x.customer_id = s.id
              AND x.country IN ('UK', 'TH')
        )
),
enriched AS (
    -- Running and moving aggregates
    SELECT
        id,
        age,
        NTILE(5) OVER (ORDER BY age DESC, id ASC) AS age_bucket,
        PERCENT_RANK() OVER w_ordered AS credit_limit_percent_rank
    FROM eligible
    WINDOW w_ordered AS (ORDER BY credit_limit ASC NULLS LAST)
),
kept AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM enriched s
    WHERE NOT EXISTS (
            SELECT 1
            FROM sessions ch
            WHERE ch.customer_id = s.id
              AND ch.device IN ('mobile', 'tablet')
        )
),
with_windows AS (
    -- Ranking and gap features
    SELECT
        id,
        age,
        ROW_NUMBER() OVER w_grp AS seq_no,
        AVG(credit_limit_percent_rank) OVER w_recent AS credit_limit_percent_rank_avg_5,
        NTILE(3) OVER w_grp AS age_bucket_2,
        CUME_DIST() OVER w_running AS age_cume_dist,
        ROW_NUMBER() OVER (ORDER BY credit_limit_percent_rank DESC, id ASC) AS seq_no_2
    FROM kept
    WHERE age_bucket < 5
    WINDOW w_grp AS (ORDER BY age DESC, id ASC), w_recent AS (ORDER BY age_bucket ASC, id ASC ROWS BETWEEN 5 PRECEDING AND CURRENT ROW), w_running AS (ORDER BY age ASC NULLS FIRST)
),
valid AS (
    -- Filter the raw rows and derive helper columns
    SELECT *, ROUND(age_cume_dist * 1.1, 2) AS age_cume_dist_scaled
    FROM with_windows
    WHERE seq_no >= 6
)
-- Multi-level aggregation
SELECT
    seq_no,
    PERCENTILE_APPROX(age_cume_dist_scaled, 0.95) AS age_cume_dist_scaled_p95,
    APPROX_PERCENTILE(seq_no, 0.95) AS seq_no_p95,
    ROUND(AVG(credit_limit_percent_rank_avg_5), 2) AS avg_credit_limit_percent_rank_avg_5
FROM valid
GROUP BY seq_no
ORDER BY seq_no_p95 DESC NULLS LAST;
