SELECT
    s.id,
    s.country,
    COUNT(p.id) product_count,
    CONCAT_WS(', ', SORT_ARRAY(COLLECT_SET(p.category))) category_values,
    MIN(p.launch_date) first_launch_date
FROM suppliers s
LEFT JOIN products p ON s.id = p.supplier_id
WHERE UPPER(s.supplier_name) LIKE '%ACI%'
GROUP BY s.id, s.country;
