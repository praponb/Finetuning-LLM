WITH matched_keys AS (
    -- Set intersection on keys
    SELECT device, id
    FROM sessions
    INTERSECT ALL
    SELECT device, id
    FROM sessions
    WHERE TO_DATE(started_at) >= '2025-02-11'
),
reconciled AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM sessions s
    JOIN matched_keys k ON s.device = k.device AND s.id = k.id
)
SELECT
    s.id,
    s.customer_id,
    s.browser,
    s.started_at,
    (SELECT AVG(x.monthly_fee) FROM subscriptions x WHERE x.customer_id = s.customer_id) AS avg_subscription_monthly_fee
FROM reconciled s
ORDER BY customer_id DESC;
