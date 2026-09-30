SELECT
    c.id,
    c.is_active,
    c.age,
    t.cnt
FROM customers c
LEFT OUTER JOIN (SELECT customer_id, COUNT(*) cnt
    FROM sessions
    GROUP BY customer_id) t ON c.id = t.customer_id
WHERE t.cnt > 4
ORDER BY cnt DESC
