SELECT
    c.id,
    c.country,
    c.segment,
    t.row_count,
    t.closed_balance
FROM customers c
LEFT OUTER JOIN (SELECT customer_id, COUNT(*) AS row_count, SUM(CASE WHEN status = 'CLOSED' THEN balance ELSE 0 END) AS closed_balance
    FROM accounts
    GROUP BY customer_id) t ON c.id = t.customer_id
