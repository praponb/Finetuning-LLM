SELECT id, opened_date
FROM stores
WHERE opened_date < (SELECT MIN(opened_date) FROM stores WHERE opened_date > '2024-04-01')
