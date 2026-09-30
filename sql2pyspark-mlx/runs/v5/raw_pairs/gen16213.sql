WITH filtered AS (
    -- Filter the raw rows and derive helper columns
    SELECT *, CASE WHEN status = 'SUCCESS' THEN 1 ELSE 0 END AS is_success
    FROM payments
    WHERE order_id IS NOT NULL
      AND method IN ('card', 'paypal')
),
with_gaps AS (
    -- Flag rows that start a new session (gap > interval)
    SELECT *, CASE WHEN paid_at - LAG(paid_at) OVER (PARTITION BY method ORDER BY paid_at, id) > INTERVAL 90 DAYS THEN 1 ELSE 0 END AS is_new_burst
    FROM filtered
),
sessions_payments AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(is_new_burst) OVER (PARTITION BY method ORDER BY paid_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS island_id
    FROM with_gaps
),
filtered_payments AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        CONCAT_WS('-', method, status) AS method_status_key,
        COALESCE(method, 'NONE') AS method_clean
    FROM sessions_payments
    WHERE paid_at > TIMESTAMP '2023-10-20 19:00:00'
      AND method IS NOT NULL
),
cleaned AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        order_id,
        method,
        amount,
        paid_at,
        status,
        is_new_burst,
        island_id,
        method_status_key,
        ROUND(amount, 2) AS amount_rounded,
        COALESCE(method, 'OTHER') AS method_clean_2
    FROM filtered_payments
    WHERE order_id IS NOT NULL
),
cleaned_payments AS (
    -- Keep recent, valid rows
    SELECT
        id,
        order_id,
        method,
        amount,
        paid_at,
        status,
        is_new_burst,
        island_id,
        method_clean_2,
        TRIM(status) AS clean_status
    FROM cleaned
    WHERE paid_at >= DATE '2023-09-02'
)
SELECT
    id,
    order_id,
    method,
    amount,
    island_id,
    method_clean_2,
    clean_status,
    CASE WHEN method RLIKE '^(bank_transfer|paypal)$' THEN 'PRIORITY' ELSE 'GROUP_A' END AS method_zone
FROM cleaned_payments
ORDER BY method;
