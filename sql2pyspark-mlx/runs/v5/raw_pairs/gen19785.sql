WITH filtered AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        DATE_SUB(signup_date, 66) AS signup_date_minus_66d,
        CONCAT(name, ', ', segment) AS name_segment
    FROM customers
    WHERE signup_date >= '2023-11-01'
)
-- Semi-join via a correlated subquery
SELECT s.*
FROM filtered s
WHERE EXISTS (
        SELECT 1
        FROM subscriptions x
        WHERE x.customer_id = s.id
          AND x.end_date > '2025-03-10'
    );
