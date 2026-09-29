SELECT
    store_id,
    YEAR(sale_date) AS sale_year,
    CASE WHEN units >= 45 THEN 'high' WHEN units >= 13 THEN 'mid' ELSE 'low' END AS units_bucket,
    MAX(sale_date) AS latest_sale_date
FROM store_sales
WHERE revenue <= 3800 AND sale_date > '2025-02-01'
GROUP BY 1, 2, 3
