WITH customer_summary AS (
    SELECT customer_id, MIN(opened_date) AS first_opened_date, ROUND((100 * SUM(CASE WHEN account_type = 'LOAN' THEN 1 ELSE 0 END)) / COUNT(*), 1) AS pct_loan
    FROM accounts
    WHERE customer_id IN (31, 39)
    GROUP BY customer_id
)
SELECT customers.is_active, customer_summary.first_opened_date, customer_summary.pct_loan
FROM customers
INNER JOIN customer_summary ON customers.id = customer_summary.customer_id
WHERE customer_summary.pct_loan >= 33;
