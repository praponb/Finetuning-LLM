WITH with_last AS (
    -- Latest matching inventory per row (LATERAL ... LIMIT 1)
    SELECT s.*, x.last_restock
    FROM products s
    LEFT JOIN LATERAL (
            SELECT c.last_restock
            FROM inventory c
            WHERE c.product_id = s.id
              AND c.last_restock <= s.launch_date
            ORDER BY c.last_restock DESC, c.id DESC
            LIMIT 1
        ) x ON TRUE
),
latest_match AS (
    -- Latest matching inventory per row (LATERAL ... LIMIT 1)
    SELECT s.*, top1.inventory_last_restock
    FROM with_last s
    LEFT JOIN LATERAL (
            SELECT c.last_restock AS inventory_last_restock
            FROM inventory c
            WHERE c.product_id = s.id
            ORDER BY c.last_restock DESC, c.id DESC
            LIMIT 1
        ) top1 ON TRUE
),
with_last_products AS (
    -- Latest matching inventory per row (LATERAL ... LIMIT 1)
    SELECT
        s.*,
        x.inventory_quantity,
        x.inventory_last_restock_2
    FROM latest_match s
    JOIN LATERAL (
            SELECT c.quantity AS inventory_quantity, c.last_restock AS inventory_last_restock_2
            FROM inventory c
            WHERE c.product_id = s.id
            ORDER BY c.quantity DESC, c.id DESC
            LIMIT 1
        ) x ON TRUE
),
with_windows AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        AVG(inventory_quantity) OVER (ORDER BY price, id ROWS BETWEEN 5 PRECEDING AND CURRENT ROW) AS inventory_quantity_avg_5,
        AVG(inventory_quantity) OVER (PARTITION BY supplier_id ORDER BY inventory_quantity, id ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING) AS inventory_quantity_avg_6_prior,
        SUM(stock_qty) OVER (PARTITION BY supplier_id ORDER BY last_restock, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_stock_qty,
        CORR(cost, price) OVER (PARTITION BY category) AS corr_cost_price,
        CORR(stock_qty, cost) OVER (PARTITION BY brand) AS corr_stock_qty_cost
    FROM with_last_products
),
enriched AS (
    -- Running and moving aggregates
    SELECT
        *,
        SUM(price) OVER (PARTITION BY category ORDER BY inventory_quantity_avg_5, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_price,
        SUM(cost) OVER (PARTITION BY category ORDER BY launch_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_cost,
        LAST_VALUE(inventory_quantity_avg_5, TRUE) OVER (PARTITION BY supplier_id ORDER BY inventory_quantity_avg_5, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_inventory_quantity_avg_5
    FROM with_windows
),
deduped AS (
    -- QUALIFY-style window filter
    SELECT *
    FROM enriched
    QUALIFY ROW_NUMBER() OVER (PARTITION BY category ORDER BY inventory_quantity DESC, id) = 1
)
SELECT
    id,
    product_name,
    category,
    brand,
    cost,
    stock_qty,
    tags,
    supplier_id,
    inventory_last_restock
FROM deduped
ORDER BY id DESC
LIMIT 20
