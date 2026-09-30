WITH enriched AS (
    SELECT
        *,
        FIRST_VALUE(quantity) IGNORE NULLS OVER w_grp AS first_known_quantity,
        SUM(quantity) OVER w_recent AS cumulative_quantity
    FROM inventory
    WINDOW w_grp AS (PARTITION BY product_id ORDER BY last_restock, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW), w_recent AS (PARTITION BY store_id ORDER BY last_restock, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
),
remaining_keys AS (
    SELECT store_id, product_id
    FROM enriched
    EXCEPT
    SELECT store_id, product_id
    FROM inventory
    WHERE quantity < 206
),
reconciled AS (
    SELECT s.*
    FROM enriched s
    INNER JOIN remaining_keys k ON s.store_id = k.store_id AND s.product_id = k.product_id
),
remaining_keys_2 AS (
    SELECT store_id, product_id
    FROM reconciled
    EXCEPT ALL
    SELECT store_id, product_id
    FROM inventory
    WHERE store_id = 6
),
verified AS (
    SELECT s.*
    FROM reconciled s
    INNER JOIN remaining_keys_2 k ON s.store_id = k.store_id AND s.product_id = k.product_id
),
valid AS (
    SELECT
        store_id,
        product_id,
        quantity,
        first_known_quantity,
        cumulative_quantity,
        CAST(cumulative_quantity AS DOUBLE) AS cumulative_quantity_num
    FROM verified
    WHERE store_id IN (2, 3, 5)
)
SELECT
    product_id,
    ROUND(AVG(cumulative_quantity_num), 2) AS avg_cumulative_quantity_num,
    ROUND(AVG(quantity), 2) AS avg_quantity,
    APPROX_PERCENTILE(cumulative_quantity, ARRAY(0.25, 0.5, 0.75)) AS cumulative_quantity_quartiles
FROM valid
GROUP BY product_id
ORDER BY product_id DESC NULLS LAST;
