SELECT *
FROM products
WHERE cost >= 240 AND brand IN ('Initech', 'Acme') AND supplier_id IS NOT NULL
ORDER BY launch_date;
