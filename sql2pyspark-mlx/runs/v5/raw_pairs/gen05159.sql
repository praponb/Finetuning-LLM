WITH valid AS (
    -- Keep recent, valid rows
    SELECT
        id,
        order_id,
        method,
        amount,
        paid_at,
        status,
        ROUND(amount * 1.07, 2) AS amount_adjusted
    FROM payments
    WHERE paid_at < '2023-12-01'
),
kept_keys AS (
    -- Set difference on keys
    SELECT order_id
    FROM valid
    EXCEPT DISTINCT
    SELECT order_id
    FROM payments
    WHERE method IS NOT NULL
),
reconciled AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM valid s
    JOIN kept_keys k ON s.order_id = k.order_id
),
joined AS (
    -- Attach orders attributes
    SELECT
        src.*,
        p.status AS order_status,
        p.channel,
        p.order_date
    FROM reconciled src
    LEFT JOIN orders p
        ON src.order_id = p.id
    WHERE p.status IN ('SHIPPED', 'RETURNED', 'CANCELLED')
),
scored AS (
    -- Window metrics over several different partitions
    SELECT
        order_status,
        channel,
        MAX(amount_adjusted) OVER (PARTITION BY status) AS status_max_amount_adjusted,
        CUME_DIST() OVER (ORDER BY amount NULLS LAST) AS amount_cume_dist,
        MAX(amount_adjusted) OVER (PARTITION BY order_status, method) AS order_status_max_amount_adjusted,
        PERCENT_RANK() OVER (PARTITION BY order_id ORDER BY amount_adjusted DESC) AS amount_adjusted_percent_rank,
        AVG(amount) OVER (ORDER BY CAST(paid_at AS BIGINT) RANGE BETWEEN 2592000 PRECEDING AND CURRENT ROW) AS amount_avg_30d
    FROM joined
)
SELECT
    order_status,
    amount_cume_dist,
    order_status_max_amount_adjusted,
    amount_adjusted_percent_rank,
    amount_avg_30d
FROM scored
