SELECT DATE_TRUNC('MONTH', opened_date) AS opened_month, CONCAT_WS(', ', SORT_ARRAY(COLLECT_SET(region))) AS region_values
FROM stores
WHERE sqft BETWEEN 11336 AND 11500
GROUP BY DATE_TRUNC('MONTH', opened_date);
