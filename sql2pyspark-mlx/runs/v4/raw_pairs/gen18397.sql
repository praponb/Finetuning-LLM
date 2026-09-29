WITH base AS (
    SELECT
        *,
        CONCAT_WS('-', status, method) AS status_method_key,
        COALESCE(method, 'OTHER') AS resolved_method
    FROM payments
    WHERE paid_at >= DATE '2023-05-24'
      AND order_id IS NOT NULL
      AND status = 'REFUNDED'
),
deduped AS (
    SELECT *
    FROM base
    QUALIFY ROW_NUMBER() OVER (PARTITION BY method ORDER BY paid_at DESC NULLS LAST, id ASC) = 1
),
base_payments AS (
    SELECT *, TRIM(method) AS clean_method
    FROM deduped
    WHERE paid_at >= TIMESTAMP '2023-11-09 14:00:00'
      AND status = 'FAILED'
),
gaps AS (
    SELECT *, CASE WHEN paid_at - LAG(paid_at) OVER (PARTITION BY status ORDER BY paid_at ASC, id ASC) > INTERVAL '14' DAY THEN 1 ELSE 0 END AS is_new_burst
    FROM base_payments
),
sessions_payments AS (
    SELECT *, SUM(is_new_burst) OVER (PARTITION BY status ORDER BY paid_at ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_no
    FROM gaps
),
gaps_2 AS (
    SELECT *, CASE WHEN paid_at - LAG(paid_at) OVER (PARTITION BY order_id ORDER BY paid_at ASC, id ASC) > INTERVAL 7 DAYS THEN 1 ELSE 0 END AS new_group_flag
    FROM sessions_payments
),
sessionized AS (
    SELECT *, SUM(new_group_flag) OVER (PARTITION BY order_id ORDER BY paid_at ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS burst_id
    FROM gaps_2
)
SELECT
    id,
    order_id,
    method,
    amount,
    status_method_key,
    resolved_method,
    clean_method,
    session_no,
    new_group_flag
FROM sessionized;
