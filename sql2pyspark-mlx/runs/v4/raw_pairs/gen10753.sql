WITH filtered AS (
    SELECT
        *,
        CASE WHEN amount >= 1450 THEN 'High' WHEN amount >= 620 THEN 'Medium' ELSE 'Low' END AS amount_tier,
        COALESCE(method, 'N/A') AS resolved_method
    FROM payments
    WHERE paid_at > TO_DATE('2023-08-09')
),
base AS (
    SELECT
        *,
        DATE_TRUNC('WEEK', paid_at) AS paid_at_period,
        INITCAP(TRIM(method)) AS method_title,
        COALESCE(method, 'DIRECT') AS resolved_method_2
    FROM filtered
    WHERE method IS NOT NULL
),
metrics AS (
    SELECT
        *,
        ROUND(amount / SUM(amount) OVER (PARTITION BY status), 4) AS amount_share,
        MIN(amount) OVER (PARTITION BY order_id, method) AS order_id_min_amount,
        ROW_NUMBER() OVER (PARTITION BY status, method ORDER BY paid_at, id) AS row_seq,
        FIRST_VALUE(amount, TRUE) OVER (PARTITION BY method, status ORDER BY amount, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_amount
    FROM base
),
with_windows AS (
    SELECT
        id,
        order_id,
        method,
        amount,
        paid_at,
        status,
        amount_tier,
        resolved_method,
        paid_at_period,
        method_title,
        resolved_method_2,
        amount_share,
        order_id_min_amount,
        row_seq,
        first_known_amount,
        COUNT(amount_share) OVER (PARTITION BY status ORDER BY CAST(paid_at AS BIGINT) RANGE BETWEEN 7776000 PRECEDING AND CURRENT ROW) AS amount_share_count_90d,
        AVG(order_id_min_amount) OVER (ORDER BY row_seq, id ROWS BETWEEN 5 PRECEDING AND CURRENT ROW) AS order_id_min_amount_avg_5,
        ROW_NUMBER() OVER (PARTITION BY status ORDER BY paid_at_period, id) AS row_seq_2
    FROM metrics
),
cleaned AS (
    SELECT
        id,
        method,
        amount,
        paid_at,
        status,
        paid_at_period,
        resolved_method_2,
        amount_share,
        order_id_min_amount,
        amount_share_count_90d,
        order_id_min_amount_avg_5,
        SUBSTRING(status, 1, 2) AS status_prefix,
        first_known_amount - amount_share AS first_known_amount_minus_amount_share
    FROM with_windows
    WHERE HOUR(paid_at_period) BETWEEN 12 AND 15
),
summary AS (
    SELECT
        status,
        CASE WHEN order_id_min_amount_avg_5 >= 1527 THEN 'LARGE' ELSE 'SMALL' END AS order_id_min_amount_avg_5_tier,
        ROUND(SUM(order_id_min_amount_avg_5) / NULLIF(COUNT(CASE WHEN status = 'FAILED' THEN 1 END), 0), 2) AS order_id_min_amount_avg_5_per_failed,
        COUNT(CASE WHEN method = 'bank_transfer' THEN 1 END) AS bank_transfer_count,
        AVG(order_id_min_amount) FILTER (WHERE method = 'promptpay') AS promptpay_avg_order_id_min_amount
    FROM cleaned
    GROUP BY 1, 2
)
SELECT
    status,
    order_id_min_amount_avg_5_tier,
    promptpay_avg_order_id_min_amount
FROM summary
ORDER BY status DESC
