WITH filtered AS (
    SELECT *, COALESCE(quantity, 0) AS quantity_filled
    FROM inventory
    WHERE product_id IN (2, 5, 8)
)
SELECT
    product_id,
    DATE_TRUNC('WEEK', last_restock) AS week_start,
    STDDEV_SAMP(quantity) AS quantity_stddev,
    ROUND(AVG(quantity_filled), 2) AS avg_quantity_filled
FROM filtered
GROUP BY 1, 2
ORDER BY product_id DESC NULLS LAST
