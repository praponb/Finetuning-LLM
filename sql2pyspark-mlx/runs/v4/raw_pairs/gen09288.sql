WITH metrics AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        customer_id,
        plan,
        start_date,
        end_date,
        monthly_fee,
        status,
        DENSE_RANK() OVER (PARTITION BY customer_id ORDER BY monthly_fee DESC NULLS FIRST) AS monthly_fee_dense_rank,
        RANK() OVER (PARTITION BY plan ORDER BY monthly_fee DESC NULLS FIRST) AS monthly_fee_rank
    FROM subscriptions
),
enriched AS (
    -- Attach customers attributes
    SELECT s.*, p.age
    FROM metrics s
    LEFT JOIN customers p
        ON s.customer_id = p.id
),
latest AS (
    -- QUALIFY-style window filter
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY status ORDER BY monthly_fee_dense_rank DESC, id) AS latest_rank
        FROM enriched
    ) x
    WHERE latest_rank = 1
)
-- Aggregate with conditional metrics
SELECT
    DATE_TRUNC('QUARTER', end_date) AS quarter_start,
    MAX(monthly_fee_dense_rank) FILTER (WHERE status = 'PAUSED') AS paused_max_monthly_fee_dense_rank,
    COUNT(CASE WHEN status = 'ACTIVE' THEN 1 END) AS active_count,
    SUM(CASE WHEN status = 'ACTIVE' THEN monthly_fee_dense_rank ELSE 0 END) AS active_monthly_fee_dense_rank,
    SUM(CASE WHEN status = 'PAUSED' THEN age ELSE 0 END) AS paused_age,
    COUNT(CASE WHEN plan = 'basic' THEN 1 END) AS basic_count
FROM latest
GROUP BY CUBE (DATE_TRUNC('QUARTER', end_date))
