WITH base AS (
    -- Keep recent, valid rows
    SELECT
        *,
        amount / NULLIF(shipping_fee, 0) AS amount_per_shipping_fee,
        COALESCE(channel, 'UNKNOWN') AS channel_clean
    FROM orders
    WHERE channel IS NOT DISTINCT FROM 'mobile'
),
with_windows AS (
    -- Ranking and gap features
    SELECT
        id,
        store_id,
        status,
        amount,
        discount,
        LEAD(discount) OVER (PARTITION BY status ORDER BY discount, id) AS next_discount,
        LAG(discount) OVER (PARTITION BY status ORDER BY discount, id) AS prev_discount,
        LEAD(amount) OVER (PARTITION BY status ORDER BY discount, id) AS next_amount,
        LAG(amount) OVER (PARTITION BY status ORDER BY discount, id) AS prev_amount,
        ROW_NUMBER() OVER (PARTITION BY status ORDER BY discount, id) AS event_seq
    FROM base
),
scored AS (
    -- Running and moving aggregates
    SELECT
        id,
        store_id,
        status,
        amount,
        next_amount,
        prev_amount,
        MAX(discount) OVER (PARTITION BY store_id ORDER BY amount, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_max_discount,
        MIN(discount) OVER (PARTITION BY store_id ORDER BY amount, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_min_discount,
        COUNT(discount) OVER (PARTITION BY store_id ORDER BY amount, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_count_discount,
        AVG(discount) OVER (PARTITION BY store_id ORDER BY amount, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_avg_discount,
        FIRST_VALUE(next_amount) IGNORE NULLS OVER (PARTITION BY store_id ORDER BY discount, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_next_amount
    FROM with_windows
),
not_flagged AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM scored s
    WHERE NOT EXISTS (
            SELECT 1
            FROM payments c
            WHERE c.order_id = s.id
              AND c.status = 'FAILED'
        )
),
with_dims AS (
    -- Attach stores attributes
    SELECT
        e.*,
        b.city,
        b.store_name
    FROM not_flagged e
    LEFT JOIN stores b
        ON e.store_id <=> b.id
)
-- Aggregate with conditional metrics
SELECT
    city,
    CASE WHEN running_min_discount >= 0.36 THEN 'LARGE' ELSE 'SMALL' END AS running_min_discount_tier,
    ROUND(AVG(running_count_discount), 2) AS avg_running_count_discount,
    MAX(amount) AS max_amount,
    MAX(prev_amount) FILTER (WHERE status = 'COMPLETED') AS completed_max_prev_amount,
    APPROX_PERCENTILE(next_amount, 0.9) AS next_amount_p90
FROM with_dims
GROUP BY city, CASE WHEN running_min_discount >= 0.36 THEN 'LARGE' ELSE 'SMALL' END
HAVING completed_max_prev_amount < 1000000
ORDER BY city DESC;
