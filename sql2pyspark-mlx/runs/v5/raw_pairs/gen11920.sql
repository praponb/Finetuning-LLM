WITH matched_keys AS (
    SELECT store_id, customer_id
    FROM orders
    EXCEPT DISTINCT
    SELECT store_id, customer_id
    FROM orders
    WHERE discount BETWEEN 0.22 AND 0.27
),
verified AS (
    SELECT s.*
    FROM orders s
    INNER JOIN matched_keys k ON s.store_id = k.store_id AND s.customer_id = k.customer_id
)
SELECT
    s.id,
    s.customer_id,
    s.store_id,
    s.channel,
    s.amount,
    s.discount,
    s.shipping_fee,
    (SELECT MAX(t2.balance) FROM accounts t2 WHERE t2.customer_id = s.customer_id) AS max_account_balance
FROM verified s;
