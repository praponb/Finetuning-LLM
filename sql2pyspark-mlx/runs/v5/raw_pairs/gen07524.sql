WITH ranked AS (
    -- QUALIFY-style window filter
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY product_id ORDER BY unit_price NULLS LAST, id) AS row_num
        FROM order_items
    ) x
    WHERE row_num = 1
)
-- Grouped metrics
SELECT
    order_id,
    product_id,
    AVG(unit_price) AS avg_unit_price,
    MIN(unit_price) AS min_unit_price,
    COUNT(*) AS event_count,
    ROUND(AVG(unit_price), 2) AS avg_unit_price_2,
    CORR(unit_price, quantity) AS corr_unit_price_quantity
FROM ranked
GROUP BY order_id, product_id
HAVING ROUND(AVG(unit_price), 2) < 1000000;
