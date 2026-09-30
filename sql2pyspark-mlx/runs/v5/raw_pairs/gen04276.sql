WITH flagged AS (
    -- Flag rows that start a new session (gap > interval)
    SELECT *, CASE WHEN paid_at - LAG(paid_at) OVER (PARTITION BY order_id ORDER BY paid_at, id) > INTERVAL '30' DAY THEN 1 ELSE 0 END AS new_group_flag
    FROM payments
),
sessionized AS (
    -- Running sum of the flag = session number
    SELECT
        id,
        order_id,
        method,
        paid_at,
        status,
        SUM(new_group_flag) OVER (PARTITION BY order_id ORDER BY paid_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS burst_id
    FROM flagged
),
scored AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        order_id,
        paid_at,
        MAX(burst_id) OVER (PARTITION BY status ORDER BY CAST(paid_at AS BIGINT) RANGE BETWEEN 604800 PRECEDING AND CURRENT ROW) AS burst_id_max_7d,
        FIRST_VALUE(method, TRUE) OVER (ORDER BY paid_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_method,
        SUM(burst_id) OVER (PARTITION BY order_id, method ORDER BY CAST(paid_at AS BIGINT) RANGE BETWEEN 7776000 PRECEDING AND CURRENT ROW) AS burst_id_sum_90d
    FROM sessionized
    WHERE method IN ('card', 'cash', 'promptpay')
),
joined AS (
    -- Attach orders attributes
    SELECT
        src.id,
        src.order_id,
        src.paid_at,
        src.burst_id_max_7d,
        src.first_known_method,
        d.discount,
        d.channel
    FROM scored src
    LEFT JOIN orders d
        ON src.order_id = d.id
),
flagged_2 AS (
    -- Flag rows that start a new session (gap > interval)
    SELECT *, CASE WHEN LAG(paid_at) OVER (PARTITION BY channel ORDER BY paid_at, id) IS NULL
                 OR paid_at > LAG(paid_at) OVER (PARTITION BY channel ORDER BY paid_at, id) + INTERVAL '7' DAY THEN 1 ELSE 0 END AS new_group_flag
    FROM joined
),
sessionized_payments AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(new_group_flag) OVER (PARTITION BY channel ORDER BY paid_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS burst_id
    FROM flagged_2
),
grouped AS (
    -- Aggregate with conditional metrics
    SELECT
        DATE_TRUNC('MONTH', paid_at) AS activity_month,
        DATE_TRUNC('WEEK', paid_at) AS week_start,
        COUNT(*) FILTER (WHERE channel = 'web') AS web_rows,
        PERCENTILE_APPROX(burst_id_max_7d, 0.5) AS burst_id_max_7d_p50,
        ROUND(AVG(discount), 2) AS avg_discount,
        DENSE_RANK() OVER (PARTITION BY DATE_TRUNC('MONTH', paid_at) ORDER BY COUNT(*) FILTER (WHERE channel = 'web') DESC) AS web_rows_dense_rank
    FROM sessionized_payments
    GROUP BY DATE_TRUNC('MONTH', paid_at), DATE_TRUNC('WEEK', paid_at)
)
SELECT
    activity_month,
    week_start,
    web_rows,
    burst_id_max_7d_p50,
    web_rows_dense_rank
FROM grouped;
