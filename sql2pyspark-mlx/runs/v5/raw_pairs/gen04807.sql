WITH cleaned AS (
    SELECT
        *,
        ADD_MONTHS(start_date, 12) AS start_date_plus_months,
        DATEDIFF(CURRENT_DATE(), start_date) AS days_since_start_date
    FROM subscriptions
    WHERE start_date >= DATE '2023-08-23'
      AND customer_id IS NOT NULL
)
SELECT id, plan
FROM cleaned
EXCEPT
SELECT id, plan
FROM subscriptions
WHERE monthly_fee BETWEEN 160 AND 170
