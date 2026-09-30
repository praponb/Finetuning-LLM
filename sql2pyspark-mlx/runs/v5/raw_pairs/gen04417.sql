WITH recent_order_items AS (
    SELECT *, CEIL(quantity) AS quantity_whole
    FROM order_items
    WHERE unit_price IS NOT NULL AND (product_id IS NULL OR quantity < 3)
),
summary AS (
    SELECT CASE WHEN unit_price >= 330 THEN 'high' WHEN unit_price >= 190 THEN 'mid' ELSE 'low' END AS unit_price_bucket, MIN(quantity) AS min_quantity
    FROM recent_order_items
    GROUP BY CASE WHEN unit_price >= 330 THEN 'high' WHEN unit_price >= 190 THEN 'mid' ELSE 'low' END
)
SELECT *
FROM summary
WHERE min_quantity >= 3.6
ORDER BY 2;
