WITH cleaned AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        customer_id,
        plan,
        start_date,
        end_date,
        status,
        UPPER(plan) plan_upper
    FROM subscriptions
    WHERE monthly_fee BETWEEN 140 AND 200
),
with_dims AS (
    -- Attach customers attributes
    SELECT s.*, c.city
    FROM cleaned s
    JOIN customers c
        ON s.customer_id = c.id
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        *,
        ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY start_date DESC, id) row_seq,
        LAST_VALUE(city, TRUE) OVER w_part last_known_city,
        LAST_VALUE(city, TRUE) OVER (PARTITION BY city, status ORDER BY end_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) last_known_city_2
    FROM with_dims
    WINDOW w_part AS (PARTITION BY customer_id ORDER BY end_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
),
base AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        plan,
        start_date,
        end_date,
        status,
        plan_upper,
        row_seq,
        last_known_city,
        DATE_TRUNC('WEEK', end_date) end_date_period,
        UPPER(city) city_upper
    FROM with_windows
    WHERE city IS NOT NULL
      AND city IN ('Berlin', 'Chiang Mai')
),
filtered AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        start_date,
        end_date,
        status,
        row_seq,
        last_known_city,
        city_upper,
        CAST(row_seq AS DOUBLE) row_seq_num
    FROM base
    WHERE start_date >= DATE '2023-04-13'
      AND status IN ('EXPIRED', 'CANCELLED', 'PAUSED')
)
-- Grouped metrics
SELECT
    CASE WHEN row_seq >= 8 THEN 'LARGE' ELSE 'SMALL' END AS row_seq_tier,
    status,
    AVG(row_seq) FILTER (WHERE status = 'PAUSED') AS paused_avg_row_seq,
    APPROX_PERCENTILE(row_seq, ARRAY(0.25, 0.5, 0.75)) AS row_seq_quartiles,
    APPROX_PERCENTILE(row_seq_num, 0.95) AS row_seq_num_p95,
    SIZE(COLLECT_SET(status)) AS num_status,
    MIN(row_seq) AS min_row_seq
FROM filtered
GROUP BY ROLLUP (CASE WHEN row_seq >= 8 THEN 'LARGE' ELSE 'SMALL' END, status)
