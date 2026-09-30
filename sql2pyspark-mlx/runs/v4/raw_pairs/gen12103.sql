WITH enriched AS (
    -- Ranking and gap features
    SELECT
        *,
        ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY started_at, id) AS row_seq,
        FIRST_VALUE(browser) IGNORE NULLS OVER (ORDER BY started_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_browser,
        FIRST_VALUE(browser) IGNORE NULLS OVER (PARTITION BY country ORDER BY started_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_browser_2
    FROM sessions
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        *,
        LAG(row_seq, 2) OVER (PARTITION BY device ORDER BY started_at, id) AS prev_row_seq,
        ROW_NUMBER() OVER (PARTITION BY browser ORDER BY started_at, id) AS row_seq_2
    FROM enriched
),
matched AS (
    -- Non-equi join on a validity window
    SELECT
        e.*,
        sub.id AS subscription_id,
        sub.plan,
        sub.monthly_fee
    FROM with_windows e
    LEFT JOIN subscriptions sub
        ON e.customer_id = sub.customer_id
       AND e.started_at BETWEEN sub.start_date AND sub.end_date
)
-- Multi-level aggregation
SELECT
    plan,
    SUM(row_seq) FILTER (WHERE plan = 'enterprise') AS enterprise_total_row_seq,
    COUNT(*) FILTER (WHERE plan = 'enterprise') AS enterprise_rows,
    MIN(started_at) AS first_seen,
    MAX(row_seq) FILTER (WHERE plan = 'premium') AS premium_max_row_seq,
    ROUND(AVG(subscription_id), 2) AS avg_subscription_id
FROM matched
GROUP BY ROLLUP (plan)
ORDER BY enterprise_total_row_seq;
