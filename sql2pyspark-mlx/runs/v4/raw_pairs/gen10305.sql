WITH metrics AS (
    -- Ranking and gap features
    SELECT
        id,
        customer_id,
        plan,
        start_date,
        end_date,
        monthly_fee,
        status,
        STDDEV_SAMP(monthly_fee) OVER (PARTITION BY plan) AS plan_stddev_samp_monthly_fee,
        SUM(monthly_fee) OVER (PARTITION BY customer_id ORDER BY end_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_monthly_fee,
        SUM(monthly_fee) OVER (PARTITION BY customer_id ORDER BY end_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_monthly_fee_2,
        AVG(monthly_fee) OVER (PARTITION BY status ORDER BY start_date, id ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING) AS monthly_fee_avg_6_prior
    FROM subscriptions
),
enriched AS (
    -- Attach customers attributes
    SELECT s.*, d.name
    FROM metrics s
    LEFT JOIN customers d
        ON s.customer_id = d.id
),
valid AS (
    -- Keep recent, valid rows
    SELECT *, ROUND(running_monthly_fee * 1.07, 2) AS adjusted_running_monthly_fee
    FROM enriched
    WHERE end_date > DATE '2023-10-13'
),
cleaned AS (
    -- Filter the raw rows and derive helper columns
    SELECT *, DATEDIFF(start_date, end_date) AS days_end_date_to_start_date
    FROM valid
    WHERE end_date >= TO_DATE('2023-09-14')
      AND customer_id IS NOT NULL
)
SELECT
    id,
    start_date,
    end_date,
    plan_stddev_samp_monthly_fee,
    running_monthly_fee,
    running_monthly_fee_2,
    monthly_fee_avg_6_prior,
    adjusted_running_monthly_fee,
    days_end_date_to_start_date
FROM cleaned
ORDER BY start_date DESC, id DESC
LIMIT 20;
