SELECT
    id,
    product_id,
    CASE WHEN units >= 31 THEN 'High' WHEN units >= 8 THEN 'Medium' ELSE 'Low' END AS units_tier,
    ROUND(units / revenue, 2) AS revenue_per_unit
FROM store_sales
WHERE revenue < 1400 AND units BETWEEN 25 AND 34 AND product_id = 6;
