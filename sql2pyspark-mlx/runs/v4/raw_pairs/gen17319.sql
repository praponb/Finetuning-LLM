WITH base AS (
    SELECT *, ADD_MONTHS(end_date, 12) AS end_date_plus_months
    FROM subscriptions
    WHERE end_date >= TO_DATE('2023-09-14')
      AND customer_id IS NOT NULL
)
SELECT
    id,
    customer_id,
    plan,
    start_date,
    end_date,
    monthly_fee,
    status,
    end_date_plus_months,
    SUM(monthly_fee) OVER (PARTITION BY status ORDER BY end_date_plus_months, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_monthly_fee,
    ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY start_date, id) AS event_seq,
    AVG(monthly_fee) OVER (PARTITION BY customer_id ORDER BY monthly_fee, id ROWS BETWEEN 5 PRECEDING AND CURRENT ROW) AS monthly_fee_avg_5
FROM base
