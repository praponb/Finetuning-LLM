WITH valid AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        store_id,
        product_id,
        quantity,
        LAST_DAY(last_restock) AS last_restock_month_end,
        ROUND(quantity, 0) AS quantity_rounded
    FROM inventory
    WHERE last_restock > DATE '2023-07-01'
),
enriched AS (
    -- Attach products attributes
    SELECT
        s.*,
        d.cost,
        d.stock_qty
    FROM valid s
    LEFT JOIN products d
        ON s.product_id = d.id
),
scored AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        store_id,
        product_id,
        quantity,
        last_restock_month_end,
        quantity_rounded,
        cost,
        stock_qty,
        ROW_NUMBER() OVER (PARTITION BY store_id ORDER BY quantity DESC, id) AS event_seq,
        LAG(quantity, 1) OVER w_part AS prev_quantity
    FROM enriched
    WINDOW w_part AS (PARTITION BY product_id ORDER BY stock_qty, id)
),
stats AS (
    -- Grouped metrics
    SELECT
        CASE WHEN quantity_rounded >= 75 THEN 'HIGH' ELSE 'LOW' END AS quantity_rounded_tier,
        COUNT(DISTINCT product_id) AS distinct_product_ids,
        COUNT(*) AS num_rows,
        ROUND(AVG(prev_quantity), 2) AS avg_prev_quantity
    FROM scored
    GROUP BY quantity_rounded_tier
)
SELECT
    quantity_rounded_tier,
    distinct_product_ids,
    num_rows,
    avg_prev_quantity
FROM stats
ORDER BY avg_prev_quantity NULLS FIRST;
