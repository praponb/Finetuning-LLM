WITH filtered AS (
    -- Basic cleansing and defaults
    SELECT *, DAYOFWEEK(end_date) AS end_date_dayofweek
    FROM subscriptions
    WHERE end_date > DATE '2023-10-12'
),
scored AS (
    -- Ranking and gap features
    SELECT
        id,
        plan,
        start_date,
        end_date,
        status,
        end_date_dayofweek,
        LEAD(end_date_dayofweek) OVER w_recent AS next_end_date_dayofweek,
        LAG(monthly_fee) OVER w_recent AS prev_monthly_fee,
        LEAD(monthly_fee) OVER w_recent AS next_monthly_fee,
        ROW_NUMBER() OVER w_recent AS event_seq
    FROM filtered
    WINDOW w_recent AS (PARTITION BY customer_id, status ORDER BY start_date, id)
),
ranked AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *
    FROM scored
    QUALIFY ROW_NUMBER() OVER (PARTITION BY plan, status ORDER BY end_date_dayofweek DESC NULLS LAST, id) = 1
),
metrics AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        start_date,
        status,
        LAG(next_monthly_fee, 2) OVER (ORDER BY end_date, id) AS prev_next_monthly_fee,
        SUM(end_date_dayofweek) OVER (PARTITION BY plan, status ORDER BY end_date_dayofweek, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_end_date_dayofweek
    FROM ranked
)
-- Aggregate with conditional metrics
SELECT
    CASE WHEN prev_next_monthly_fee >= 198 THEN 'PREMIUM' ELSE 'STANDARD' END AS prev_next_monthly_fee_tier,
    DATE_TRUNC('QUARTER', start_date) AS quarter_start,
    COUNT(CASE WHEN status = 'EXPIRED' THEN 1 END) AS expired_count,
    PERCENTILE_APPROX(prev_next_monthly_fee, 0.5) AS prev_next_monthly_fee_p50,
    SIZE(COLLECT_SET(status)) AS num_status,
    PERCENTILE_APPROX(prev_next_monthly_fee, 0.9) AS prev_next_monthly_fee_p90
FROM metrics
GROUP BY CASE WHEN prev_next_monthly_fee >= 198 THEN 'PREMIUM' ELSE 'STANDARD' END, DATE_TRUNC('QUARTER', start_date)
HAVING prev_next_monthly_fee_p50 > 0
ORDER BY prev_next_monthly_fee_tier NULLS LAST;
