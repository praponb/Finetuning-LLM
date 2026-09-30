WITH metrics AS (
    -- Ranking and gap features
    SELECT
        id,
        start_date,
        end_date,
        AVG(monthly_fee) OVER (PARTITION BY status) AS status_avg_monthly_fee,
        FIRST_VALUE(monthly_fee, TRUE) OVER (PARTITION BY plan ORDER BY end_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_monthly_fee,
        AVG(monthly_fee) OVER (PARTITION BY status) AS status_avg_monthly_fee_2,
        CUME_DIST() OVER (ORDER BY monthly_fee DESC NULLS FIRST) AS monthly_fee_cume_dist,
        RANK() OVER (PARTITION BY plan ORDER BY monthly_fee DESC NULLS LAST) AS monthly_fee_rank
    FROM subscriptions
    WHERE start_date < '2024-09-01'
)
SELECT
    id,
    end_date,
    monthly_fee_cume_dist,
    monthly_fee_rank
FROM metrics;
