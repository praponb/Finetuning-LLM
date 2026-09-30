SELECT s.country, s.rating, p.stock_qty, p.brand, p.price FROM suppliers s LEFT JOIN products p ON p.supplier_id = s.id WHERE s.country = 'TH' AND s.rating <= 3.4 ORDER BY s.rating;
