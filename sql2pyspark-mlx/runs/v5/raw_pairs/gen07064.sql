SELECT
    c.id,
    c.segment,
    c.name,
    c.country
FROM customers c
WHERE c.segment IS NULL AND EXISTS (SELECT 1 FROM sessions s WHERE s.customer_id = c.id);
