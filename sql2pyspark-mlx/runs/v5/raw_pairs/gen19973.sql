SELECT
    c.id,
    c.is_active,
    c.email,
    t.frozen_count
FROM customers c
LEFT OUTER JOIN (SELECT customer_id, COUNT(CASE WHEN status = 'FROZEN' THEN 1 END) frozen_count
    FROM accounts
    WHERE customer_id IS NULL
    GROUP BY customer_id) t ON c.id = t.customer_id;
