WITH with_windows AS (
    SELECT
        *,
        RANK() OVER (PARTITION BY store_id ORDER BY quantity NULLS FIRST) AS quantity_rank,
        NTILE(5) OVER (PARTITION BY store_id ORDER BY quantity DESC, id) AS quantity_bucket,
        LEAD(quantity, 1) OVER w_ordered AS next_quantity
    FROM inventory
    WHERE quantity <= 180
    WINDOW w_ordered AS (PARTITION BY product_id ORDER BY quantity, id)
)
SELECT
    product_id,
    store_id,
    MIN(quantity_rank) AS min_quantity_rank,
    ROUND(AVG(quantity_bucket), 2) AS avg_quantity_bucket,
    PERCENTILE_APPROX(quantity_bucket, 0.9) AS quantity_bucket_p90,
    BIT_AND(quantity) AS quantity_bit_and,
    PERCENTILE_APPROX(quantity_bucket, 0.5) AS quantity_bucket_p50,
    APPROX_PERCENTILE(quantity, ARRAY(0.25, 0.5, 0.75)) AS quantity_quartiles
FROM with_windows
GROUP BY product_id, store_id
HAVING PERCENTILE_APPROX(quantity_bucket, 0.5) < 1000000
ORDER BY quantity_bucket_p90 DESC;
