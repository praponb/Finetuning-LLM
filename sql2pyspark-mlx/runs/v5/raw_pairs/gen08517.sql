WITH filtered AS (
    -- Basic cleansing and defaults
    SELECT *, YEAR(start_date) AS start_date_year
    FROM subscriptions
    WHERE start_date > DATE '2023-03-11'
      AND plan != 'standard'
)
-- Running and moving aggregates
SELECT
    id,
    plan,
    start_date,
    monthly_fee,
    start_date_year,
    AVG(start_date_year) OVER (PARTITION BY status ORDER BY start_date, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS start_date_year_avg_6,
    LEAD(start_date_year, 1) OVER w_recent AS next_start_date_year
FROM filtered
WHERE customer_id IN (3, 20, 27, 35)
WINDOW w_recent AS (PARTITION BY plan ORDER BY start_date_year, id);
