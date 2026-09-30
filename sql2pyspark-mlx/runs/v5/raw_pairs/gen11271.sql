WITH deduped AS (
    -- QUALIFY-style window filter
    SELECT *, RANK() OVER (PARTITION BY order_id, status ORDER BY paid_at) AS rn
    FROM payments
    QUALIFY rn = 1
)
-- Aggregate with conditional metrics
SELECT
    CASE WHEN amount >= 1540 THEN 'HIGH' ELSE 'LOW' END AS amount_tier,
    DATE_TRUNC('WEEK', paid_at) AS week_start,
    MIN(amount) AS min_amount,
    MAX(paid_at) AS last_paid_at,
    COUNT(CASE WHEN method = 'bank_transfer' THEN 1 END) AS bank_transfer_count,
    SORT_ARRAY(COLLECT_SET(status)) AS status_list
FROM deduped
GROUP BY 1, 2
ORDER BY week_start DESC
