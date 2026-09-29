SELECT c.email, c.credit_limit, s.end_date
FROM customers c
INNER JOIN subscriptions s ON c.id = s.customer_id
WHERE c.segment NOT IN ('Consumer', 'Corporate', 'SMB') AND c.city <> 'New York'
ORDER BY email
