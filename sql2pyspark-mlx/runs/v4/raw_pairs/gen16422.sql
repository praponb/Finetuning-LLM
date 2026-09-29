SELECT c.city, COUNT(CASE WHEN s.device = 'desktop' THEN 1 END) AS desktop_count, COUNT(DISTINCT s.customer_id) AS unique_customers
FROM sessions s
INNER JOIN customers c ON c.id = s.customer_id
WHERE c.segment IN ('Consumer', 'Corporate', 'SMB')
GROUP BY c.city
