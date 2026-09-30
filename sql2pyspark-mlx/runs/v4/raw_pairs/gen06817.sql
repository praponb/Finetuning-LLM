SELECT s.country, s.rating, s.city, p.cost, p.supplier_id FROM suppliers s LEFT OUTER JOIN products p ON s.id = p.supplier_id WHERE p.product_name LIKE '%Bott%' AND s.city != 'Chiang Mai';
