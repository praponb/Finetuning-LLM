SELECT * FROM (SELECT *, ROW_NUMBER() OVER (PARTITION BY category ORDER BY cost, id) AS rnk FROM products WHERE brand NOT IN ('Globex', 'Initech')) AS ranked WHERE rnk <= 5
