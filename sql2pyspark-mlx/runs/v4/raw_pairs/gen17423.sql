WITH valid AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        CASE WHEN amount >= 1590 THEN 'High' WHEN amount >= 530 THEN 'Medium' ELSE 'Low' END AS amount_band,
        CASE WHEN method = 'cash' THEN 1 ELSE 0 END AS is_cash,
        COALESCE(method, 'N/A') AS resolved_method
    FROM payments
    WHERE method IS NOT NULL
),
scored AS (
    -- Ranking and gap features
    SELECT
        *,
        MIN(is_cash) OVER (PARTITION BY status) AS status_min_is_cash,
        ROUND(amount / SUM(amount) OVER (PARTITION BY status, order_id), 4) AS amount_share,
        AVG(is_cash) OVER (PARTITION BY method, status ORDER BY paid_at, id ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING) AS is_cash_avg_3_prior,
        NTILE(10) OVER (ORDER BY is_cash DESC, id) AS is_cash_bucket
    FROM valid
),
enriched AS (
    -- Ranking and gap features
    SELECT
        *,
        ROUND(amount / SUM(amount) OVER (PARTITION BY order_id), 4) AS amount_share_2,
        NTILE(10) OVER (PARTITION BY order_id ORDER BY is_cash, id) AS is_cash_bucket_2,
        AVG(amount_share) OVER (PARTITION BY status ORDER BY status_min_is_cash, id ROWS BETWEEN 5 PRECEDING AND CURRENT ROW) AS amount_share_avg_5
    FROM scored
),
gaps AS (
    -- Flag rows that start a new session (gap > interval)
    SELECT *, CASE WHEN LAG(paid_at) OVER (PARTITION BY status ORDER BY paid_at, id) IS NULL
                 OR paid_at > LAG(paid_at) OVER (PARTITION BY status ORDER BY paid_at, id) + INTERVAL '30' DAY THEN 1 ELSE 0 END AS is_new_burst
    FROM enriched
),
islands AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(is_new_burst) OVER (PARTITION BY status ORDER BY paid_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_id
    FROM gaps
)
-- Multi-level aggregation
SELECT
    CASE WHEN is_cash >= 87 THEN 'LARGE' ELSE 'SMALL' END AS is_cash_tier,
    order_id,
    COUNT(*) AS num_rows,
    PERCENTILE_APPROX(status_min_is_cash, ARRAY(0.25, 0.5, 0.75)) AS status_min_is_cash_quartiles,
    SUM(CASE WHEN method = 'bank_transfer' THEN amount_share ELSE 0 END) AS bank_transfer_amount_share,
    MAX(paid_at) AS last_paid_at
FROM islands
GROUP BY CASE WHEN is_cash >= 87 THEN 'LARGE' ELSE 'SMALL' END, order_id;
