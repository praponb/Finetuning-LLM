WITH latest AS (
    SELECT
        id,
        store_id,
        quantity
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY product_id ORDER BY quantity DESC, id) AS rn
        FROM inventory
    ) t
    WHERE rn = 1
)
SELECT
    CASE WHEN quantity >= 204 THEN 'LARGE' ELSE 'SMALL' END AS quantity_tier,
    MIN(quantity) AS min_quantity,
    PERCENTILE_APPROX(quantity, ARRAY(0.25, 0.5, 0.75)) AS quantity_quartiles,
    STDDEV_SAMP(quantity) AS quantity_stddev
FROM latest
GROUP BY CASE WHEN quantity >= 204 THEN 'LARGE' ELSE 'SMALL' END
HAVING STDDEV_SAMP(quantity) >= 0
ORDER BY min_quantity DESC;
