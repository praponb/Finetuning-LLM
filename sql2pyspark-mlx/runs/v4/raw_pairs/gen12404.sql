WITH eligible AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM orders s
    WHERE NOT EXISTS (
            SELECT 1
            FROM payments ch
            WHERE ch.order_id = s.id
              AND ch.paid_at <= s.order_date
        )
),
with_window AS (
    -- Non-equi join on a validity window
    SELECT
        a.*,
        pr.id AS promo_id,
        pr.promo_code,
        pr.discount_pct
    FROM eligible a
    INNER JOIN promotions pr
        ON a.channel = pr.channel
       AND a.order_date >= pr.starts_on
       AND a.order_date < pr.ends_on + INTERVAL 30 DAYS
),
remaining_keys AS (
    -- Set difference on keys
    SELECT customer_id, channel
    FROM with_window
    EXCEPT ALL
    SELECT customer_id, channel
    FROM orders
    WHERE order_date >= '2024-06-16'
),
reconciled AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM with_window s
    INNER JOIN remaining_keys k ON s.customer_id = k.customer_id AND s.channel = k.channel
)
-- Aggregate with conditional metrics
SELECT
    status,
    COUNT(DISTINCT channel) AS distinct_channels,
    COUNT(*) AS cnt,
    COUNT(*) FILTER (WHERE channel = 'mobile') AS mobile_rows
FROM reconciled
GROUP BY CUBE (status)
