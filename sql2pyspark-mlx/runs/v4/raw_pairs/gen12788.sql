SELECT
    city,
    MAX(opened_date) AS latest_opened_date,
    ROUND(AVG(sqft), 2) AS avg_sqft,
    ROUND((100 * SUM(CASE WHEN city = 'Bangkok' THEN 1 ELSE 0 END)) / COUNT(*), 1) AS pct_bangkok
FROM stores
WHERE NOT (store_name LIKE 'Si%' AND store_name LIKE '%Expr%' AND opened_date <= '2025-05-01')
GROUP BY city
