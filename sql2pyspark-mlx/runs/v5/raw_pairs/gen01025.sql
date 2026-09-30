WITH scored AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY amount ASC, id ASC) AS seq_no,
        FIRST_VALUE(method) IGNORE NULLS OVER (PARTITION BY status, order_id ORDER BY paid_at ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_method,
        COUNT(amount) OVER (ORDER BY CAST(paid_at AS BIGINT) ASC RANGE BETWEEN 2592000 PRECEDING AND CURRENT ROW) AS amount_count_30d
    FROM payments
),
kept_keys AS (
    -- Set intersection on keys
    SELECT method, id
    FROM scored
    INTERSECT
    SELECT method, id
    FROM payments
    WHERE status IN ('REFUNDED', 'SUCCESS', 'FAILED')
),
kept_rows AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM scored s
    INNER JOIN kept_keys k ON s.method = k.method AND s.id = k.id
),
valid AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        order_id,
        method,
        amount,
        paid_at,
        status,
        seq_no,
        first_known_method,
        amount_count_30d,
        ROUND(amount, 2) AS amount_rounded
    FROM kept_rows
    WHERE paid_at >= DATE '2023-06-11'
      AND method IS DISTINCT FROM 'card'
),
joined AS (
    -- Attach orders attributes
    SELECT src.*, o.amount AS order_amount
    FROM valid src
    INNER JOIN orders o
        ON src.order_id <=> o.id
),
with_dims AS (
    -- Attach orders attributes
    SELECT e.*, d.status AS order_status
    FROM joined e
    LEFT JOIN orders d
        ON e.order_id <=> d.id
    WHERE d.store_id IS NULL
),
valid_payments AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        order_id,
        paid_at,
        status,
        seq_no,
        first_known_method,
        amount_count_30d,
        amount_rounded,
        order_amount,
        order_status,
        ROUND(amount * 100, 2) AS amount_adjusted
    FROM with_dims
    WHERE amount > 690
),
metrics AS (
    -- Ranking and gap features
    SELECT
        *,
        AVG(amount_adjusted) OVER w_hist AS amount_adjusted_avg_30d,
        SUM(amount_rounded) OVER w_ordered AS order_status_sum_amount_rounded,
        SUM(amount_adjusted) OVER w_hist AS amount_adjusted_sum_30d
    FROM valid_payments
    WINDOW w_hist AS (PARTITION BY order_status ORDER BY CAST(paid_at AS BIGINT) ASC RANGE BETWEEN 2592000 PRECEDING AND CURRENT ROW), w_ordered AS (PARTITION BY order_status)
)
-- Native SQL UNPIVOT (wide to long)
SELECT
    order_id,
    metric,
    metric_value
FROM (
    SELECT
        order_id,
        amount_adjusted,
        order_amount,
        amount_rounded
    FROM metrics
) src
UNPIVOT (
    metric_value FOR metric IN (amount_adjusted, order_amount, amount_rounded)
)
