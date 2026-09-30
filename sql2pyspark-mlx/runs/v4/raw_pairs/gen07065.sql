WITH cleaned AS (
    SELECT
        *,
        ROUND(amount * 0.9, 2) AS amount_scaled,
        CONCAT(status, ' - ', method) AS status_method
    FROM payments
    WHERE paid_at > TIMESTAMP '2023-06-12 01:00:00'
      AND method IS NOT NULL
),
flagged AS (
    SELECT *, CASE WHEN LAG(paid_at) OVER (PARTITION BY status ORDER BY paid_at, id) IS NULL
                 OR paid_at > LAG(paid_at) OVER (PARTITION BY status ORDER BY paid_at, id) + INTERVAL '14' DAY THEN 1 ELSE 0 END AS is_new_burst
    FROM cleaned
),
islands AS (
    SELECT *, SUM(is_new_burst) OVER (PARTITION BY status ORDER BY paid_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_no
    FROM flagged
),
metrics AS (
    SELECT
        id,
        order_id,
        method,
        amount,
        paid_at,
        status,
        amount_scaled,
        status_method,
        is_new_burst,
        session_no,
        AVG(session_no) OVER (PARTITION BY method) AS method_avg_session_no,
        COUNT(amount_scaled) OVER (PARTITION BY status ORDER BY CAST(paid_at AS BIGINT) RANGE BETWEEN 604800 PRECEDING AND CURRENT ROW) AS amount_scaled_count_7d,
        ROW_NUMBER() OVER (ORDER BY amount, id) AS event_seq,
        FIRST_VALUE(amount, TRUE) OVER (PARTITION BY order_id ORDER BY amount, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_amount,
        LAST_VALUE(method) IGNORE NULLS OVER (PARTITION BY method ORDER BY is_new_burst, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_method
    FROM islands
),
deduped AS (
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY status, method ORDER BY amount_scaled DESC NULLS LAST, id) AS row_num
        FROM metrics
    ) ranked
    WHERE row_num = 1
),
scored AS (
    SELECT
        *,
        AVG(session_no) OVER (PARTITION BY method ORDER BY is_new_burst, id ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING) AS session_no_avg_2_prior,
        SUM(amount) OVER (PARTITION BY method ORDER BY paid_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_amount,
        NTILE(10) OVER (PARTITION BY method ORDER BY amount_scaled DESC, id) AS amount_scaled_bucket
    FROM deduped
),
filtered AS (
    SELECT
        id,
        order_id,
        method,
        amount,
        paid_at,
        status,
        amount_scaled,
        is_new_burst,
        session_no,
        method_avg_session_no,
        event_seq,
        first_known_amount,
        last_known_method,
        row_num,
        session_no_avg_2_prior,
        cumulative_amount,
        amount_scaled_bucket,
        FLOOR(cumulative_amount) AS cumulative_amount_whole,
        CAST(event_seq AS DOUBLE) AS event_seq_str
    FROM scored
    WHERE paid_at > TIMESTAMP '2024-04-21 19:00:00'
),
filtered_payments AS (
    SELECT
        *,
        INITCAP(TRIM(method)) AS method_title,
        COALESCE(method, 'N/A') AS resolved_method
    FROM filtered
    WHERE event_seq BETWEEN 4 AND 5
)
SELECT
    id,
    order_id,
    method,
    amount,
    paid_at,
    status,
    amount_scaled,
    is_new_burst,
    session_no,
    method_avg_session_no,
    event_seq,
    first_known_amount,
    last_known_method,
    row_num,
    session_no_avg_2_prior,
    cumulative_amount,
    amount_scaled_bucket,
    cumulative_amount_whole,
    event_seq_str,
    method_title,
    resolved_method,
    SUM(session_no_avg_2_prior) OVER (PARTITION BY method, status ORDER BY method_avg_session_no, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_session_no_avg_2_prior,
    NTILE(4) OVER (ORDER BY amount DESC, id) AS amount_bucket,
    SUM(cumulative_amount) OVER (PARTITION BY order_id ORDER BY first_known_amount, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_cumulative_amount
FROM filtered_payments;
