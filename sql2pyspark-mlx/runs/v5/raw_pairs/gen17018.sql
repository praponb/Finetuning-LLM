WITH enriched AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        order_id,
        amount,
        paid_at,
        LAG(amount, 1) OVER (PARTITION BY status ORDER BY paid_at, id) AS prev_amount,
        COUNT(amount) OVER (PARTITION BY method ORDER BY CAST(paid_at AS BIGINT) RANGE BETWEEN 604800 PRECEDING AND CURRENT ROW) AS amount_count_7d,
        SUM(amount) OVER (PARTITION BY status, method ORDER BY CAST(paid_at AS BIGINT) RANGE BETWEEN 2592000 PRECEDING AND CURRENT ROW) AS amount_sum_30d
    FROM payments
),
joined AS (
    -- Attach orders attributes
    SELECT a.*, b.channel
    FROM enriched a
    JOIN orders b
        ON a.order_id = b.id
),
joined_payments AS (
    -- Attach orders attributes
    SELECT
        s.id,
        s.order_id,
        s.paid_at,
        s.amount_sum_30d,
        s.channel,
        d.discount,
        d.amount AS order_amount
    FROM joined s
    JOIN orders d
        ON s.order_id <=> d.id
),
with_prev AS (
    -- Previous event time per key
    SELECT *, LAG(paid_at) OVER (PARTITION BY channel ORDER BY paid_at, id) AS prev_paid_at
    FROM joined_payments
),
flagged AS (
    -- Flag rows that start a new session
    SELECT *, CASE WHEN prev_paid_at IS NULL OR paid_at > prev_paid_at + INTERVAL '7' DAY THEN 1 ELSE 0 END AS is_new_burst
    FROM with_prev
),
islands AS (
    -- Running sum of the flag = session number
    SELECT
        id,
        order_id,
        paid_at,
        channel,
        order_amount,
        SUM(is_new_burst) OVER (PARTITION BY channel ORDER BY paid_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_no
    FROM flagged
),
ranked AS (
    -- Keep one row per key (latest first)
    SELECT *
    FROM (
        SELECT *, RANK() OVER (PARTITION BY channel ORDER BY order_amount) AS latest_rank
        FROM islands
    ) ranked
    WHERE latest_rank = 1
),
attributed AS (
    -- Non-equi join on a validity window
    SELECT
        e.*,
        pr.id AS promo_id,
        pr.promo_code,
        pr.discount_pct
    FROM ranked e
    JOIN promotions pr
        ON COALESCE(e.channel, 'store') = pr.channel
       AND e.paid_at >= pr.starts_on
       AND e.paid_at < pr.ends_on + INTERVAL '14' DAY
),
daily AS (
    -- Aggregate with conditional metrics
    SELECT
        CASE WHEN order_amount >= 1300 THEN 'PREMIUM' ELSE 'STANDARD' END AS order_amount_tier,
        CASE WHEN session_no >= 8 THEN 'PREMIUM' ELSE 'STANDARD' END AS session_no_tier,
        DATE_TRUNC('QUARTER', paid_at) AS quarter_start,
        ROUND(AVG(promo_id), 2) AS avg_promo_id,
        COUNT(*) FILTER (WHERE channel = 'mobile') AS mobile_rows,
        COUNT(*) AS num_rows,
        SUM(CASE WHEN promo_code = 'WELCOME5' THEN promo_id ELSE 0 END) AS welcome5_promo_id
    FROM attributed
    GROUP BY GROUPING SETS ((CASE WHEN order_amount >= 1300 THEN 'PREMIUM' ELSE 'STANDARD' END, CASE WHEN session_no >= 8 THEN 'PREMIUM' ELSE 'STANDARD' END, DATE_TRUNC('QUARTER', paid_at)), (CASE WHEN order_amount >= 1300 THEN 'PREMIUM' ELSE 'STANDARD' END, CASE WHEN session_no >= 8 THEN 'PREMIUM' ELSE 'STANDARD' END), ())
)
SELECT
    order_amount_tier,
    session_no_tier,
    quarter_start,
    avg_promo_id,
    mobile_rows,
    num_rows,
    welcome5_promo_id
FROM daily
ORDER BY avg_promo_id DESC NULLS FIRST;
