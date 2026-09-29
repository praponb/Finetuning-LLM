WITH cleaned AS (
    SELECT
        *,
        ROUND(quantity / unit_price, 2) AS line_total,
        CASE WHEN unit_price >= 300 THEN 'High' WHEN unit_price >= 110 THEN 'Medium' ELSE 'Low' END AS unit_price_level
    FROM order_items
    WHERE product_id IS NOT NULL
)
SELECT
    id,
    quantity,
    unit_price_level
FROM (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY product_id, order_id ORDER BY line_total, id) AS dedup_rank
    FROM cleaned
) ranked
WHERE dedup_rank = 1
