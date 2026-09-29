WITH customer_totals AS (
    SELECT customer_id, COUNT(CASE WHEN country = 'TH' THEN 1 END) AS th_count
    FROM sessions
    GROUP BY customer_id
)
SELECT c.name, c.segment, ct.th_count
FROM customers c
INNER JOIN customer_totals ct ON c.id = ct.customer_id;
