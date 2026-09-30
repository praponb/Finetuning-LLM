SELECT
    city,
    CASE WHEN rating >= 3.5 THEN 'high' WHEN rating >= 2.4 THEN 'mid' ELSE 'low' END rating_bucket,
    COUNT(*) num_suppliers,
    CONCAT_WS(', ', SORT_ARRAY(COLLECT_SET(country))) country_values
FROM suppliers
WHERE city IS NULL AND rating IS NULL
GROUP BY city, CASE WHEN rating >= 3.5 THEN 'high' WHEN rating >= 2.4 THEN 'mid' ELSE 'low' END
ORDER BY 4;
