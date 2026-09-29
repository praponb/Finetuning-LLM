SELECT
    id,
    sale_date,
    store_id,
    units,
    ROUND(revenue, 0) revenue_rounded
FROM store_sales
WHERE revenue BETWEEN 2400 AND 3200 AND units <= 14
ORDER BY store_id DESC;
