WITH filtered AS (
    -- Keep recent, valid rows
    SELECT
        id,
        customer_id,
        plan,
        start_date,
        monthly_fee,
        LAST_DAY(end_date) AS end_date_month_end
    FROM subscriptions
    WHERE customer_id IS NOT NULL
      AND end_date <= '2025-02-01'
)
SELECT customer_id
FROM filtered
EXCEPT
SELECT customer_id
FROM subscriptions
WHERE end_date >= '2023-10-01';
