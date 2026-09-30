SELECT AVG(price) AS avg_price FROM products WHERE product_name LIKE '%Offic%' AND brand NOT IN ('Stark', 'Initech', 'Acme') ORDER BY avg_price DESC;
