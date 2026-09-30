WITH metrics AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        MAX(amount) OVER w_part AS running_max_amount,
        COUNT(amount) OVER w_part AS running_count_amount,
        SUM(amount) OVER w_part AS running_sum_amount,
        CUME_DIST() OVER w_part_2 AS amount_cume_dist
    FROM payments
    WINDOW w_part AS (PARTITION BY order_id ORDER BY amount ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW), w_part_2 AS (PARTITION BY method ORDER BY amount DESC)
)
SELECT
    id,
    amount,
    status,
    running_max_amount,
    running_sum_amount,
    CASE WHEN status RLIKE '^(REFUNDED|FAILED)$' THEN 'PRIORITY' WHEN status RLIKE '^(SUCCESS)$' THEN 'CORE' ELSE 'GROUP_B' END AS status_zone
FROM metrics;
