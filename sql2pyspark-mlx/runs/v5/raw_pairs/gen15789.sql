WITH cleaned AS (
    SELECT
        id,
        plan,
        start_date,
        end_date,
        monthly_fee,
        status,
        SUBSTRING(status, 1, 4) AS status_prefix
    FROM subscriptions
    WHERE customer_id = 15
)
SELECT plan, id
FROM cleaned
EXCEPT DISTINCT
SELECT plan, id
FROM subscriptions
WHERE YEAR(start_date) >= 2024;
