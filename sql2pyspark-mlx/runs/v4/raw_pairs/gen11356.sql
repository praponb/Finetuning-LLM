SELECT
    id,
    store_id,
    units,
    LEAST(revenue, units) AS least_revenue_units
FROM store_sales
WHERE (revenue <= 3800 AND store_id IN (5, 6)) OR revenue BETWEEN 3600 AND 4000
ORDER BY least_revenue_units DESC, id ASC
LIMIT 10;
