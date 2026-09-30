WITH CustomerTotals AS (
    SELECT customer_id, SORT_ARRAY(COLLECT_SET(account_type)) AS account_type_list
    FROM accounts
    WHERE balance >= 32000
    GROUP BY customer_id
)
SELECT c.id, c.signup_date, C.account_type_list
FROM customers c
INNER JOIN CustomerTotals C ON c.id = C.customer_id;
