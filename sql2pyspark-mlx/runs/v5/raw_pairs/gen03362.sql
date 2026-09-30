WITH CustomerTotals AS (
    SELECT customer_id, CONCAT_WS(', ', SORT_ARRAY(COLLECT_SET(account_type))) AS account_type_values, MAX(opened_date) AS latest_opened_date
    FROM accounts
    WHERE balance IS NULL
    GROUP BY customer_id
)
SELECT
    c.age,
    c.credit_limit,
    C.account_type_values,
    C.latest_opened_date
FROM customers c
INNER JOIN CustomerTotals C ON c.id = C.customer_id
