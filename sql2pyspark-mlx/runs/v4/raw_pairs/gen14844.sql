WITH base AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        method,
        amount,
        status,
        CASE WHEN status = 'SUCCESS' THEN 1 ELSE 0 END AS is_success,
        COALESCE(method, 'DIRECT') AS method_clean
    FROM payments
    WHERE paid_at > TO_DATE('2024-04-12')
      AND order_id IS NOT NULL
      AND TO_DATE(paid_at) >= '2024-07-27'
),
with_windows AS (
    -- Ranking and gap features
    SELECT
        id,
        method,
        amount,
        status,
        is_success,
        method_clean,
        AVG(is_success) OVER (PARTITION BY method ORDER BY is_success, id ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING) AS is_success_avg_6_prior,
        AVG(is_success) OVER (PARTITION BY method, status ORDER BY amount, id ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING) AS is_success_avg_6_prior_2
    FROM base
),
valid AS (
    -- Filter the raw rows and derive helper columns
    SELECT *, CONCAT_WS(',', method, status) AS method_status_key
    FROM with_windows
    WHERE method IS NOT NULL
),
enriched AS (
    -- Running and moving aggregates
    SELECT
        *,
        NTILE(4) OVER (PARTITION BY method ORDER BY is_success DESC, id) AS is_success_bucket,
        CUME_DIST() OVER w_ordered AS is_success_cume_dist,
        ROW_NUMBER() OVER w_running AS row_seq,
        NTILE(5) OVER w_grp AS is_success_avg_6_prior_bucket,
        AVG(is_success) OVER (PARTITION BY status ORDER BY is_success_avg_6_prior, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS is_success_avg_2
    FROM valid
    WINDOW w_ordered AS (PARTITION BY status, method ORDER BY is_success DESC), w_running AS (ORDER BY is_success_avg_6_prior, id), w_grp AS (PARTITION BY status ORDER BY is_success_avg_6_prior DESC, id)
),
valid_payments AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        CASE WHEN is_success >= 60 THEN 'High' WHEN is_success >= 31 THEN 'Medium' ELSE 'Low' END AS is_success_band,
        row_seq + is_success_avg_6_prior_2 AS row_seq_plus_is_success_avg_6_prior_2,
        COALESCE(method, 'NONE') AS method_filled
    FROM enriched
    WHERE method IS NOT NULL
      AND method IS NULL
)
-- Multi-level aggregation
SELECT
    method,
    status,
    CASE WHEN is_success_avg_2 >= 82 THEN 'HIGH' ELSE 'LOW' END AS is_success_avg_2_tier,
    COUNT(*) FILTER (WHERE status = 'REFUNDED') AS refunded_rows,
    ROUND(AVG(is_success_avg_6_prior), 2) AS avg_is_success_avg_6_prior,
    COUNT_IF(status = 'SUCCESS') AS success_events
FROM valid_payments
GROUP BY method, status, CASE WHEN is_success_avg_2 >= 82 THEN 'HIGH' ELSE 'LOW' END
ORDER BY method;
