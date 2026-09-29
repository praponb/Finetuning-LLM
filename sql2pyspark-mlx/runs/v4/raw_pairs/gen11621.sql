SELECT country, COUNT(*) AS supplier_count
FROM suppliers
WHERE supplier_name LIKE 'L%'
GROUP BY country
ORDER BY country DESC, supplier_count ASC
