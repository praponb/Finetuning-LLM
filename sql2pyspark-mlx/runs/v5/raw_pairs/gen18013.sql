WITH cleaned AS (
    -- Keep recent, valid rows
    SELECT
        *,
        LAST_DAY(start_date) AS start_date_month_end,
        CASE WHEN monthly_fee >= 184 THEN 'High' WHEN monthly_fee >= 89 THEN 'Medium' ELSE 'Low' END AS monthly_fee_level
    FROM subscriptions
    WHERE end_date > TO_DATE('2023-08-11')
      AND customer_id IS NOT NULL
)
-- Deduplicate with ROW_NUMBER
SELECT *
FROM cleaned
QUALIFY ROW_NUMBER() OVER (PARTITION BY plan ORDER BY end_date DESC NULLS LAST, id) = 1;
