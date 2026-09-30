SELECT
    units,
    product_id,
    revenue,
    ROUND(units * 1.15, 2) adjusted_units
FROM store_sales
WHERE revenue > 3800 AND units >= 40 AND product_id IN (3, 7, 9);
