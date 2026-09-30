SELECT s.city, p.category, p.product_name FROM suppliers s INNER JOIN products p ON p.supplier_id = s.id WHERE SIZE(p.tags) >= 2 ORDER BY category DESC;
