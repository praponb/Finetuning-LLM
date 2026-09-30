WITH base AS (
    -- Keep recent, valid rows
    SELECT
        id,
        order_id,
        method,
        amount,
        paid_at,
        status,
        CASE WHEN amount >= 1260 THEN 'High' WHEN amount >= 650 THEN 'Medium' ELSE 'Low' END AS amount_band,
        UPPER(status) AS status_upper
    FROM payments
    WHERE method IS DISTINCT FROM 'promptpay'
      AND method <> 'promptpay'
),
scored AS (
    -- Ranking and gap features
    SELECT
        *,
        SUM(amount) OVER (ORDER BY amount, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_amount,
        AVG(amount) OVER (PARTITION BY status ORDER BY amount, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS amount_avg_3,
        COUNT(amount) OVER (PARTITION BY method, order_id ORDER BY CAST(paid_at AS BIGINT) RANGE BETWEEN 2592000 PRECEDING AND CURRENT ROW) AS amount_count_30d,
        CUME_DIST() OVER (PARTITION BY method ORDER BY amount) AS amount_cume_dist,
        NTILE(3) OVER (ORDER BY amount DESC, id) AS amount_bucket
    FROM base
    WHERE status = 'REFUNDED'
),
joined AS (
    -- Attach orders attributes
    SELECT
        e.*,
        b.order_date,
        b.shipping_fee,
        b.amount AS order_amount
    FROM scored e
    LEFT JOIN orders b
        ON e.order_id = b.id
),
matched_keys AS (
    -- Set intersection on keys
    SELECT status
    FROM joined
    INTERSECT ALL
    SELECT status
    FROM payments
    WHERE status = 'SUCCESS'
),
reconciled AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM joined s
    JOIN matched_keys k ON s.status = k.status
),
valid AS (
    -- Filter the raw rows and derive helper columns
    SELECT *, DATEDIFF(CURRENT_DATE(), paid_at) AS days_since_paid_at
    FROM reconciled
    WHERE shipping_fee <= 11
)
SELECT
    order_id,
    method,
    amount,
    paid_at,
    status,
    amount_band,
    status_upper,
    running_amount,
    amount_avg_3,
    CASE WHEN method RLIKE '^(paypal|cash)$' THEN 'OTHER_ZONE' ELSE 'GROUP_A' END AS method_zone
FROM valid
