WITH deduped AS (
    -- Keep one row per key (latest first)
    SELECT *, RANK() OVER (PARTITION BY customer_id ORDER BY order_date) AS rn
    FROM orders
    QUALIFY rn = 1
),
kept_keys AS (
    -- Set intersection on keys
    SELECT id
    FROM deduped
    INTERSECT
    SELECT id
    FROM orders
    WHERE discount > 0.35
),
reconciled AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM deduped s
    JOIN kept_keys k ON s.id = k.id
),
with_dims AS (
    -- Attach customers attributes
    SELECT
        a.customer_id,
        a.amount,
        a.shipping_fee,
        a.rn,
        p.city,
        p.name,
        p.signup_date
    FROM reconciled a
    LEFT JOIN customers p
        ON a.customer_id <=> p.id
),
with_windows AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        CUME_DIST() OVER (PARTITION BY city, customer_id ORDER BY shipping_fee DESC NULLS FIRST) AS shipping_fee_cume_dist,
        CUME_DIST() OVER (PARTITION BY customer_id ORDER BY amount DESC NULLS FIRST) AS amount_cume_dist
    FROM with_dims
    WHERE YEAR(signup_date) >= 2023
),
joined AS (
    -- Attach customers attributes
    SELECT s.*, c.segment
    FROM with_windows s
    LEFT JOIN customers c
        ON s.customer_id = c.id
)
-- Aggregate with conditional metrics
SELECT
    DATE_TRUNC('WEEK', signup_date) AS week_start,
    customer_id,
    CASE WHEN amount_cume_dist >= 0.71 THEN 'HIGH' ELSE 'LOW' END AS amount_cume_dist_tier,
    COUNT(*) AS cnt,
    COUNT(*) FILTER (WHERE city = 'Berlin') AS berlin_rows,
    ROUND(SUM(amount_cume_dist) / NULLIF(COUNT(CASE WHEN city = 'Tokyo' THEN 1 END), 0), 2) AS amount_cume_dist_per_tokyo,
    COUNT(CASE WHEN segment = 'Corporate' THEN 1 END) AS corporate_count,
    ROUND(SUM(amount_cume_dist) / NULLIF(COUNT(CASE WHEN segment = 'Consumer' THEN 1 END), 0), 2) AS amount_cume_dist_per_consumer
FROM joined
GROUP BY week_start, customer_id, amount_cume_dist_tier
HAVING cnt >= 1
ORDER BY week_start DESC;
