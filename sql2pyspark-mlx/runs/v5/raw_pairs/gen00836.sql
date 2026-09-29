WITH valid AS (
    SELECT
        id,
        customer_id,
        opened_date,
        status,
        ROUND(balance * 1.1, 2) AS balance_scaled
    FROM accounts
    WHERE opened_date > DATE '2023-06-17'
)
SELECT
    e.id,
    e.customer_id,
    e.opened_date,
    e.status,
    e.balance_scaled,
    b.segment
FROM valid e
LEFT JOIN customers b
    ON e.customer_id = b.id;
