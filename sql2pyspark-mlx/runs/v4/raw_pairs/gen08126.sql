WITH base AS (
    SELECT *, COALESCE(quantity, 0) AS quantity_filled
    FROM inventory
    WHERE last_restock >= DATE '2023-09-10'
),
filtered AS (
    SELECT *, CASE WHEN quantity_filled >= 60 THEN 'High' WHEN quantity_filled >= 14 THEN 'Medium' ELSE 'Low' END AS quantity_filled_tier
    FROM base
    WHERE last_restock >= DATE '2024-05-03'
),
deduped AS (
    SELECT *
    FROM filtered
    QUALIFY ROW_NUMBER() OVER (PARTITION BY product_id ORDER BY quantity_filled NULLS LAST, id) = 1
),
ranked AS (
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY store_id, product_id ORDER BY quantity_filled NULLS LAST, id) AS rank_in_group
        FROM deduped
    ) ranked
    WHERE rank_in_group = 1
),
filtered_inventory AS (
    SELECT *, LEAST(quantity, quantity_filled) AS least_quantity_quantity_filled
    FROM ranked
    WHERE quantity_filled > 70
),
scored AS (
    SELECT
        *,
        SUM(least_quantity_quantity_filled) OVER (PARTITION BY store_id, product_id ORDER BY quantity, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_least_quantity_quantity_filled,
        DENSE_RANK() OVER (PARTITION BY product_id ORDER BY quantity_filled DESC NULLS FIRST) AS quantity_filled_dense_rank,
        LAST_VALUE(quantity_filled) IGNORE NULLS OVER (ORDER BY rank_in_group, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_quantity_filled,
        SUM(least_quantity_quantity_filled) OVER w_grp AS running_least_quantity_quantity_filled
    FROM filtered_inventory
    WINDOW w_grp AS (ORDER BY last_restock, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
),
deduped_inventory AS (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY store_id, product_id ORDER BY quantity NULLS FIRST, id) AS rank_in_group_2
    FROM scored
    QUALIFY rank_in_group_2 = 1
)
SELECT
    product_id,
    CASE WHEN quantity_filled_dense_rank >= 7 THEN 'PREMIUM' ELSE 'STANDARD' END AS quantity_filled_dense_rank_tier,
    ROUND(AVG(last_known_quantity_filled), 2) AS avg_last_known_quantity_filled,
    MAX(running_least_quantity_quantity_filled) AS max_running_least_quantity_quantity_filled
FROM deduped_inventory
GROUP BY GROUPING SETS ((product_id, CASE WHEN quantity_filled_dense_rank >= 7 THEN 'PREMIUM' ELSE 'STANDARD' END), (product_id), ())
HAVING avg_last_known_quantity_filled > 0
ORDER BY max_running_least_quantity_quantity_filled
