WITH base AS (
    -- Keep recent, valid rows
    SELECT
        id,
        order_id,
        method,
        amount,
        status,
        FLOOR(amount) AS amount_whole
    FROM payments
    WHERE paid_at >= TO_TIMESTAMP('2023-05-12 11:00:00')
),
deduped AS (
    -- QUALIFY-style window filter
    SELECT
        id,
        order_id,
        method,
        amount
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY amount_whole DESC NULLS LAST, id) AS row_num
        FROM base
    ) t
    WHERE row_num = 1
),
cleaned AS (
    -- Keep recent, valid rows
    SELECT *, ROUND(amount * 1.07, 2) AS adjusted_amount
    FROM deduped
    WHERE order_id IS NOT NULL
),
with_windows AS (
    -- Ranking and gap features
    SELECT
        *,
        LEAD(amount) OVER (PARTITION BY method, order_id ORDER BY amount, id) AS next_amount,
        LAG(amount) OVER (PARTITION BY method, order_id ORDER BY amount, id) AS prev_amount,
        LEAD(adjusted_amount) OVER (PARTITION BY method, order_id ORDER BY amount, id) AS next_adjusted_amount,
        ROW_NUMBER() OVER (PARTITION BY method, order_id ORDER BY amount, id) AS row_seq
    FROM cleaned
),
enriched AS (
    -- Running and moving aggregates
    SELECT
        *,
        AVG(row_seq) OVER (ORDER BY adjusted_amount, id ROWS BETWEEN 5 PRECEDING AND CURRENT ROW) AS row_seq_avg_5,
        LAST_VALUE(prev_amount, TRUE) OVER (PARTITION BY order_id ORDER BY adjusted_amount, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_prev_amount
    FROM with_windows
),
grouped AS (
    -- Aggregate with conditional metrics
    SELECT
        order_id,
        CASE WHEN adjusted_amount >= 67 THEN 'PREMIUM' ELSE 'STANDARD' END AS adjusted_amount_tier,
        CASE WHEN amount >= 1650 THEN 'HIGH' ELSE 'LOW' END AS amount_tier,
        SORT_ARRAY(COLLECT_SET(method)) AS method_list,
        COUNT(DISTINCT method) AS distinct_methods,
        COUNT(CASE WHEN method = 'paypal' THEN 1 END) AS paypal_count
    FROM enriched
    GROUP BY CUBE (order_id, CASE WHEN adjusted_amount >= 67 THEN 'PREMIUM' ELSE 'STANDARD' END, CASE WHEN amount >= 1650 THEN 'HIGH' ELSE 'LOW' END)
)
SELECT
    amount_tier,
    distinct_methods,
    paypal_count
FROM grouped
