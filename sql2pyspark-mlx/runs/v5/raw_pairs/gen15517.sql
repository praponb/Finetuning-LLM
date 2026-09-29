SELECT s.country, s.city, p.supplier_id, COALESCE(s.rating, 0) AS rating_filled FROM suppliers s LEFT JOIN products p ON p.supplier_id = s.id WHERE s.rating >= 2.8 ORDER BY p.supplier_id DESC
