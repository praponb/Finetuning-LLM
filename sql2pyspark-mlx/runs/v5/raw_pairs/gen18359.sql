WITH valid AS (
    SELECT
        id,
        order_id,
        product_id,
        unit_price,
        ROUND(unit_price * 100, 2) AS unit_price_scaled,
        CASE WHEN quantity >= 8 THEN 'High' WHEN quantity >= 3 THEN 'Medium' ELSE 'Low' END AS quantity_level
    FROM order_items
    WHERE order_id = 5
)
SELECT
    id,
    order_id,
    unit_price,
    unit_price_scaled,
    COUNT(unit_price) OVER (PARTITION BY order_id) AS order_id_count_unit_price,
    RANK() OVER (PARTITION BY order_id ORDER BY unit_price NULLS FIRST) AS unit_price_rank,
    LAST_VALUE(unit_price, TRUE) OVER (PARTITION BY order_id ORDER BY unit_price_scaled, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_unit_price
FROM valid
WHERE product_id IS NULL
