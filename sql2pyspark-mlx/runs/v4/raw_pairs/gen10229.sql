WITH base AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        customer_id,
        plan,
        start_date,
        end_date,
        monthly_fee,
        status,
        DATE_TRUNC('month', end_date) AS end_date_period,
        DATE_ADD(end_date, 31) AS end_date_plus_31d
    FROM subscriptions
    WHERE end_date > DATE '2023-05-24'
      AND customer_id IS NOT NULL
),
flagged AS (
    -- Flag rows that start a new session (gap > interval)
    SELECT *, CASE WHEN end_date_period - LAG(end_date_period) OVER (PARTITION BY plan ORDER BY end_date_period, id) > INTERVAL 14 DAYS THEN 1 ELSE 0 END AS is_new_session
    FROM base
),
sessions_subscriptions AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(is_new_session) OVER (PARTITION BY plan ORDER BY end_date_period, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS island_id
    FROM flagged
),
deduped AS (
    -- QUALIFY-style window filter
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY status ORDER BY end_date_plus_31d DESC NULLS FIRST, id) AS latest_rank
        FROM sessions_subscriptions
    ) x
    WHERE latest_rank = 1
),
deduped_subscriptions AS (
    -- Keep one row per key (latest first)
    SELECT *, ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY end_date_period DESC NULLS FIRST, id) AS row_num
    FROM deduped
    QUALIFY row_num = 1
),
enriched AS (
    -- Attach customers attributes
    SELECT
        a.*,
        p.age,
        p.country
    FROM deduped_subscriptions a
    LEFT JOIN customers p
        ON a.customer_id = p.id
)
-- Grouped metrics
SELECT
    DATE_TRUNC('MONTH', start_date) AS activity_month,
    APPROX_PERCENTILE(island_id, 0.95) AS island_id_p95,
    SIZE(COLLECT_SET(status)) AS num_status
FROM enriched
GROUP BY activity_month
ORDER BY num_status NULLS FIRST;
