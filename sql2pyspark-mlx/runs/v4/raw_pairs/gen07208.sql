WITH cleaned AS (
    -- Basic cleansing and defaults
    SELECT *, CONCAT(status, ' - ', method) AS status_method
    FROM payments
    WHERE method IS NOT NULL
),
deduped AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY method ORDER BY paid_at DESC NULLS LAST, id ASC) AS dedup_rank
        FROM cleaned
    ) ranked
    WHERE dedup_rank = 1
),
cleaned_payments AS (
    -- Keep recent, valid rows
    SELECT
        id,
        order_id,
        method,
        paid_at,
        status,
        status_method,
        dedup_rank,
        CASE WHEN amount >= 1220 THEN 'High' WHEN amount >= 700 THEN 'Medium' ELSE 'Low' END AS amount_band,
        COALESCE(method, 'N/A') AS resolved_method
    FROM deduped
    WHERE method IS NOT NULL
),
filtered AS (
    -- Keep recent, valid rows
    SELECT *, CASE WHEN status = 'SUCCESS' THEN 1 ELSE 0 END AS is_success
    FROM cleaned_payments
    WHERE paid_at >= DATE '2024-04-17'
      AND order_id IS NOT NULL
      AND method IN ('card', 'paypal')
),
grouped AS (
    -- Aggregate with conditional metrics
    SELECT
        DATE_TRUNC('MONTH', paid_at) AS activity_month,
        status,
        COUNT_IF(method = 'card') AS card_events,
        MIN(paid_at) AS first_paid_at,
        ROUND(AVG(is_success), 2) AS avg_is_success
    FROM filtered
    GROUP BY ROLLUP (DATE_TRUNC('MONTH', paid_at), status)
)
SELECT
    activity_month,
    card_events,
    first_paid_at,
    avg_is_success
FROM grouped
ORDER BY avg_is_success DESC, first_paid_at ASC
