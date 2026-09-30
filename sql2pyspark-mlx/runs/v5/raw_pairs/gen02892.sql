WITH filtered AS (
    SELECT
        id,
        store_id,
        product_id,
        quantity,
        last_restock,
        CASE WHEN quantity >= 237 THEN 'High' WHEN quantity >= 111 THEN 'Medium' ELSE 'Low' END AS quantity_band
    FROM inventory
    WHERE product_id IN (2, 6, 7, 10)
),
with_windows AS (
    SELECT
        id,
        store_id,
        product_id,
        last_restock,
        PERCENT_RANK() OVER (PARTITION BY store_id ORDER BY quantity DESC NULLS FIRST) AS quantity_percent_rank,
        AVG(quantity) OVER (PARTITION BY product_id ORDER BY quantity, id ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING) AS quantity_avg_6_prior,
        CUME_DIST() OVER (PARTITION BY store_id ORDER BY quantity DESC NULLS LAST) AS quantity_cume_dist
    FROM filtered
    WHERE last_restock < '2024-04-23'
),
valid AS (
    SELECT
        *,
        ROUND(quantity_avg_6_prior * 100, 2) AS adjusted_quantity_avg_6_prior,
        CAST(quantity_cume_dist AS INT) AS quantity_cume_dist_int
    FROM with_windows
    WHERE quantity_cume_dist BETWEEN 0.64 AND 0.66
),
deduped AS (
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY product_id ORDER BY quantity_percent_rank DESC NULLS LAST, id) AS rn
        FROM valid
    ) ranked
    WHERE rn = 1
)
SELECT
    d.region,
    COUNT(*) AS num_rows,
    MAX(e.quantity_percent_rank) AS max_quantity_percent_rank,
    SUM(e.adjusted_quantity_avg_6_prior) AS total_adjusted_quantity_avg_6_prior
FROM deduped e
INNER JOIN stores d ON e.store_id = d.id
GROUP BY d.region
