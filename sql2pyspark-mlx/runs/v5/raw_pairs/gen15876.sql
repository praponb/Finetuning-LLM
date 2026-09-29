WITH CustomerSummary AS (
    SELECT customer_id, MAX(opened_date) last_opened_date, COUNT(DISTINCT balance) unique_statuss
    FROM accounts
    GROUP BY customer_id
)
SELECT
    c.age,
    c.country,
    C.last_opened_date,
    C.unique_statuss
FROM customers c
LEFT OUTER JOIN CustomerSummary C ON c.id = C.customer_id
WHERE C.unique_statuss > 6
ORDER BY C.unique_statuss;
