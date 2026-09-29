WITH by_dimension AS (
    -- Join a dimension and aggregate
    SELECT
        c.city,
        c.segment,
        COUNT(*) AS num_rows,
        ROUND(AVG(a.balance), 2) AS avg_balance
    FROM accounts a
    JOIN customers c ON a.customer_id = c.id
    GROUP BY c.city, c.segment
)
SELECT
    city,
    segment,
    num_rows,
    avg_balance
FROM by_dimension;
