SELECT city, 'supplier' AS source FROM suppliers UNION ALL SELECT city, 'store' AS source FROM stores WHERE opened_date BETWEEN '2024-07-25' AND '2025-09-16' ORDER BY source;
