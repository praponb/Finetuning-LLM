WITH valid AS (
    SELECT
        id,
        order_id,
        method,
        paid_at,
        status,
        CASE WHEN amount >= 1780 THEN 'High' WHEN amount >= 330 THEN 'Medium' ELSE 'Low' END AS amount_band
    FROM payments
    WHERE paid_at >= TIMESTAMP '2023-09-23 19:00:00'
      AND amount BETWEEN 500 AND 1100
),
enriched AS (
    SELECT
        src.id,
        src.order_id,
        src.paid_at,
        src.status,
        src.amount_band,
        p.amount
    FROM valid src
    INNER JOIN orders p
        ON src.order_id = p.id
),
metrics AS (
    SELECT
        id,
        order_id,
        paid_at,
        status,
        AVG(amount) OVER (PARTITION BY status ORDER BY paid_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_avg_amount,
        MAX(amount) OVER (PARTITION BY status ORDER BY paid_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_max_amount,
        SUM(amount) OVER (PARTITION BY status ORDER BY paid_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_sum_amount,
        COUNT(amount) OVER (PARTITION BY status ORDER BY paid_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_count_amount,
        FIRST_VALUE(amount, TRUE) OVER (ORDER BY amount, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_amount
    FROM enriched
),
enriched_payments AS (
    SELECT
        id,
        paid_at,
        running_max_amount,
        running_sum_amount,
        running_count_amount,
        first_known_amount,
        AVG(running_max_amount) OVER (PARTITION BY order_id, status ORDER BY running_max_amount, id ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING) AS running_max_amount_avg_3_prior,
        AVG(running_avg_amount) OVER (PARTITION BY order_id, status ORDER BY running_avg_amount, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS running_avg_amount_avg_2
    FROM metrics
    WHERE HOUR(paid_at) BETWEEN 11 AND 15
),
metrics_payments AS (
    SELECT
        *,
        AVG(running_count_amount) OVER w_running AS last3_avg_running_count_amount,
        COUNT(running_count_amount) OVER w_running AS last3_count_running_count_amount,
        MIN(running_count_amount) OVER w_running AS last3_min_running_count_amount,
        SUM(running_count_amount) OVER w_running AS last3_sum_running_count_amount,
        PERCENT_RANK() OVER (ORDER BY running_sum_amount DESC NULLS FIRST) AS running_sum_amount_percent_rank
    FROM enriched_payments
    WINDOW w_running AS (ORDER BY running_sum_amount, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW)
)
SELECT
    id,
    paid_at,
    running_max_amount,
    running_sum_amount,
    running_count_amount,
    first_known_amount,
    running_max_amount_avg_3_prior,
    last3_count_running_count_amount,
    last3_sum_running_count_amount
FROM metrics_payments
ORDER BY running_max_amount_avg_3_prior DESC NULLS LAST, running_sum_amount DESC
