SELECT
    id,
    store_id,
    revenue,
    sale_date,
    ROUND(revenue * 0.9, 2) AS revenue_adjusted
FROM store_sales
WHERE NOT (revenue <= 1600 AND sale_date >= '2025-05-01' AND sale_date > '2023-12-01');
