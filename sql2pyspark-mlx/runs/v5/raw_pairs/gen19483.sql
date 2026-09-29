SELECT p.id, p.launch_date
FROM products p
WHERE p.launch_date < '2025-05-31' AND p.supplier_id IN (SELECT id FROM suppliers WHERE country IN ('UK', 'TH'));
