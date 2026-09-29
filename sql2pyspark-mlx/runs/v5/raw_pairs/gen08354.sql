WITH filtered AS (
    -- Filter the raw rows and derive helper columns
    SELECT *, ADD_MONTHS(last_restock, 1) last_restock_plus_months
    FROM inventory
    WHERE store_id = 4
),
scored AS (
    -- Ranking and gap features
    SELECT
        *,
        SUM(quantity) OVER (PARTITION BY product_id ORDER BY last_restock, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) running_quantity,
        LEAD(quantity, 1) OVER (PARTITION BY store_id ORDER BY quantity, id) next_quantity,
        COUNT(quantity) OVER (PARTITION BY store_id) store_id_count_quantity
    FROM filtered
),
with_windows AS (
    -- Ranking and gap features
    SELECT
        id,
        store_id,
        product_id,
        running_quantity,
        AVG(store_id_count_quantity) OVER (PARTITION BY product_id) product_id_avg_store_id_count_quantity,
        MAX(running_quantity) OVER w_running store_id_max_running_quantity,
        NTILE(3) OVER w_recent store_id_count_quantity_bucket
    FROM scored
    WINDOW w_running AS (PARTITION BY store_id, product_id), w_recent AS (PARTITION BY product_id, store_id ORDER BY store_id_count_quantity DESC, id)
),
enriched AS (
    -- Attach stores attributes
    SELECT
        e.id,
        e.store_id,
        e.product_id,
        e.running_quantity,
        e.store_id_max_running_quantity,
        e.store_id_count_quantity_bucket,
        p.opened_date,
        p.region
    FROM with_windows e
    LEFT JOIN stores p
        ON e.store_id = p.id
    WHERE p.sqft != 4837
),
with_dims AS (
    -- Attach stores attributes
    SELECT
        e.*,
        s.store_name,
        s.opened_date AS store_opened_date,
        s.city
    FROM enriched e
    LEFT JOIN stores s
        ON e.store_id = s.id
),
remaining_keys AS (
    -- Set difference on keys
    SELECT id, store_id
    FROM with_dims
    EXCEPT
    SELECT id, store_id
    FROM inventory
    WHERE store_id = 2
),
verified AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM with_dims s
    JOIN remaining_keys k ON s.id = k.id AND s.store_id = k.store_id
),
with_dims_inventory AS (
    -- Attach products attributes
    SELECT src.*, b.price
    FROM verified src
    JOIN products b
        ON src.product_id = b.id
)
-- Native SQL PIVOT
SELECT *
FROM (
    SELECT
        store_id,
        city,
        region,
        price
    FROM with_dims_inventory
) src
PIVOT (
    SUM(price)
    FOR region IN ('Central' AS central, 'East' AS east)
);
