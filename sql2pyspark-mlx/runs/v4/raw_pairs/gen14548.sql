WITH latest AS (
    -- Keep one row per key (latest first)
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY brand ORDER BY price, id) AS latest_rank
        FROM products
    ) x
    WHERE latest_rank = 1
),
latest_products AS (
    -- Keep one row per key (latest first)
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY brand, category ORDER BY latest_rank DESC, id) AS dedup_rank
        FROM latest
    ) ranked
    WHERE dedup_rank = 1
),
with_latest AS (
    -- Latest matching inventory per row (LATERAL ... LIMIT 1)
    SELECT s.*, top1.inventory_quantity
    FROM latest_products s
    LEFT JOIN LATERAL (
            SELECT c.quantity AS inventory_quantity
            FROM inventory c
            WHERE c.product_id = s.id
            ORDER BY c.last_restock DESC, c.id DESC
            LIMIT 1
        ) top1 ON TRUE
),
scored AS (
    -- Ranking and gap features
    SELECT
        *,
        SUM(cost) OVER (PARTITION BY supplier_id ORDER BY launch_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_cost,
        ROW_NUMBER() OVER (PARTITION BY category, brand ORDER BY latest_rank DESC, id) AS seq_no,
        LAST_VALUE(stock_qty, TRUE) OVER (ORDER BY stock_qty, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_stock_qty
    FROM with_latest
),
eligible AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM scored s
    WHERE NOT EXISTS (
            SELECT 1
            FROM order_items ch
            WHERE ch.product_id = s.id
              AND ch.unit_price BETWEEN 260 AND 280
        )
)
-- Multi-level aggregation
SELECT
    supplier_id,
    COUNT(*) FILTER (WHERE category = 'Sports') AS sports_rows,
    SIZE(COLLECT_SET(category)) AS num_categories,
    COUNT(*) FILTER (WHERE category = 'Electronics') AS electronics_rows
FROM eligible
GROUP BY supplier_id
HAVING num_categories < 1
ORDER BY num_categories DESC NULLS LAST;
