WITH base AS (
    SELECT
        id,
        store_id,
        product_id,
        quantity,
        last_restock,
        COALESCE(quantity, 0) AS quantity_filled
    FROM inventory
    WHERE product_id IN (1, 5, 7, 9)
),
with_dims AS (
    SELECT
        a.*,
        p.stock_qty,
        p.brand,
        p.category
    FROM base a
    LEFT JOIN products p
        ON a.product_id = p.id
),
enriched AS (
    SELECT
        id,
        store_id,
        product_id,
        quantity,
        last_restock,
        quantity_filled,
        stock_qty,
        brand,
        category,
        AVG(stock_qty) OVER (PARTITION BY product_id ORDER BY last_restock, id ROWS BETWEEN 5 PRECEDING AND CURRENT ROW) AS stock_qty_avg_5,
        PERCENT_RANK() OVER (PARTITION BY category ORDER BY quantity_filled DESC) AS quantity_filled_percent_rank,
        ROW_NUMBER() OVER w_recent AS row_seq,
        MAX(quantity_filled) OVER w_grp AS store_id_max_quantity_filled,
        COUNT(quantity) OVER (PARTITION BY store_id, category) AS store_id_count_quantity
    FROM with_dims
    WINDOW w_recent AS (PARTITION BY category ORDER BY stock_qty, id), w_grp AS (PARTITION BY store_id)
)
SELECT
    id,
    store_id,
    product_id,
    quantity,
    last_restock,
    stock_qty,
    brand,
    category,
    store_id_count_quantity
FROM enriched
ORDER BY category, brand DESC
