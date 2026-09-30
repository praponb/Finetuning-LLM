WITH valid AS (
    SELECT *, CASE WHEN quantity >= 260 THEN 'High' WHEN quantity >= 31 THEN 'Medium' ELSE 'Low' END AS quantity_tier
    FROM inventory
    WHERE store_id IN (2, 3, 4, 6)
),
kept_keys AS (
    SELECT id, store_id
    FROM valid
    INTERSECT
    SELECT id, store_id
    FROM inventory
    WHERE store_id IN (1, 5)
),
verified AS (
    SELECT s.*
    FROM valid s
    INNER JOIN kept_keys k ON s.id = k.id AND s.store_id = k.store_id
),
filtered AS (
    SELECT
        id,
        store_id,
        product_id,
        quantity,
        last_restock,
        quantity_tier,
        CASE WHEN quantity >= 183 THEN 'High' WHEN quantity >= 113 THEN 'Medium' ELSE 'Low' END AS quantity_level
    FROM verified
    WHERE last_restock >= DATE '2023-07-13'
),
enriched AS (
    SELECT
        id,
        store_id,
        product_id,
        quantity,
        quantity_tier,
        quantity_level,
        PERCENT_RANK() OVER (ORDER BY quantity) AS quantity_percent_rank,
        CUME_DIST() OVER w_recent AS quantity_cume_dist
    FROM filtered
    WINDOW w_recent AS (PARTITION BY product_id ORDER BY quantity DESC NULLS FIRST)
),
with_windows AS (
    SELECT
        id,
        store_id,
        quantity,
        quantity_tier,
        quantity_level,
        quantity_percent_rank,
        CUME_DIST() OVER (ORDER BY quantity_percent_rank DESC NULLS LAST) AS quantity_percent_rank_cume_dist,
        PERCENT_RANK() OVER (ORDER BY quantity) AS quantity_percent_rank_2
    FROM enriched
),
valid_inventory AS (
    SELECT *, quantity_percent_rank_2 + quantity_percent_rank_cume_dist AS quantity_percent_rank_2_plus_quantity_percent_rank_cume_dist
    FROM with_windows
    WHERE quantity > 222
),
kept_keys_2 AS (
    SELECT store_id, id
    FROM valid_inventory
    INTERSECT ALL
    SELECT store_id, id
    FROM inventory
    WHERE YEAR(last_restock) = 2025
),
kept_rows AS (
    SELECT s.*
    FROM valid_inventory s
    INNER JOIN kept_keys_2 k ON s.store_id = k.store_id AND s.id = k.id
)
SELECT
    s.id,
    s.store_id,
    s.quantity,
    s.quantity_level,
    s.quantity_percent_rank,
    s.quantity_percent_rank_2,
    s.quantity_percent_rank_2_plus_quantity_percent_rank_cume_dist,
    (SELECT AVG(t2.quantity) FROM inventory t2 WHERE t2.store_id = s.store_id) AS avg_inventory_quantity
FROM kept_rows s
ORDER BY avg_inventory_quantity
