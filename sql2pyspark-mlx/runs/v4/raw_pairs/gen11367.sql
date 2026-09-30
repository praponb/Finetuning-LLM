WITH base AS (
    SELECT
        id,
        order_id,
        quantity,
        unit_price,
        CASE WHEN unit_price >= 420 THEN 'High' WHEN unit_price >= 150 THEN 'Medium' ELSE 'Low' END AS unit_price_band
    FROM order_items
    WHERE product_id IS NOT NULL
)
SELECT *
FROM base
QUALIFY ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY unit_price DESC NULLS FIRST, id) = 1;
