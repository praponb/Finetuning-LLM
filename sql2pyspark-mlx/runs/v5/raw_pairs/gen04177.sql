WITH with_windows AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        MAX(discount) OVER (PARTITION BY status) status_max_discount,
        SUM(amount) OVER w_grp running_amount
    FROM orders
    WHERE discount < 0.12
    WINDOW w_grp AS (ORDER BY amount, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
),
filtered AS (
    -- Basic cleansing and defaults
    SELECT *, ROUND(discount * 100, 2) discount_scaled
    FROM with_windows
    WHERE customer_id IS NOT NULL
      AND order_date > DATE '2023-05-18'
),
remaining_keys AS (
    -- Set intersection on keys
    SELECT channel, customer_id
    FROM filtered
    INTERSECT
    SELECT channel, customer_id
    FROM orders
    WHERE discount BETWEEN 0.13 AND 0.34
),
reconciled AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM filtered s
    INNER JOIN remaining_keys k ON s.channel = k.channel AND s.customer_id = k.customer_id
),
joined AS (
    -- Attach customers attributes
    SELECT
        src.customer_id,
        src.store_id,
        src.order_date,
        src.channel,
        src.amount,
        src.status_max_discount,
        src.running_amount,
        src.discount_scaled,
        b.signup_date
    FROM reconciled src
    LEFT JOIN customers b
        ON src.customer_id <=> b.id
),
attributed AS (
    -- Non-equi join on a validity window
    SELECT
        s.*,
        pr.id AS promo_id,
        pr.promo_code,
        pr.discount_pct
    FROM joined s
    LEFT JOIN promotions pr
        ON s.channel = pr.channel
       AND s.signup_date BETWEEN pr.starts_on AND pr.ends_on
)
-- Aggregate with conditional metrics
SELECT
    DATE_TRUNC('MONTH', signup_date) AS activity_month,
    store_id,
    PERCENTILE_APPROX(discount_scaled, 0.95) AS discount_scaled_p95,
    SIZE(COLLECT_SET(promo_code)) AS num_promo_codes
FROM attributed
GROUP BY ROLLUP (DATE_TRUNC('MONTH', signup_date), store_id)
HAVING discount_scaled_p95 > 0
