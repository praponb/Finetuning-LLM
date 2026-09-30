WITH remaining_keys AS (
    -- Set difference on keys
    SELECT id, product_id
    FROM order_items
    EXCEPT
    SELECT id, product_id
    FROM order_items
    WHERE product_id IN (14, 37)
),
reconciled AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM order_items s
    INNER JOIN remaining_keys k ON s.id = k.id AND s.product_id = k.product_id
),
enriched AS (
    -- Attach products attributes
    SELECT s.*, d.brand
    FROM reconciled s
    LEFT JOIN products d
        ON s.product_id = d.id
),
with_windows AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        order_id,
        product_id,
        quantity,
        unit_price,
        brand,
        ROUND(quantity / SUM(quantity) OVER (PARTITION BY product_id, brand), 4) AS quantity_share,
        ROUND(quantity / SUM(quantity) OVER (PARTITION BY brand), 4) AS quantity_share_2,
        CORR(unit_price, quantity) OVER (PARTITION BY brand) AS corr_unit_price_quantity,
        DENSE_RANK() OVER (ORDER BY quantity DESC) AS quantity_dense_rank
    FROM enriched
),
enriched_order_items AS (
    -- Attach products attributes
    SELECT s.*, p.product_name
    FROM with_windows s
    LEFT JOIN products p
        ON s.product_id = p.id
),
enriched_order_items_2 AS (
    -- Ranking and gap features
    SELECT
        id,
        order_id,
        product_id,
        quantity,
        unit_price,
        brand,
        quantity_share,
        quantity_share_2,
        corr_unit_price_quantity,
        quantity_dense_rank,
        product_name,
        COUNT(quantity) OVER (PARTITION BY brand) AS brand_count_quantity,
        RANK() OVER (PARTITION BY brand ORDER BY corr_unit_price_quantity DESC) AS corr_unit_price_quantity_rank
    FROM enriched_order_items
),
with_windows_order_items AS (
    -- Running and moving aggregates
    SELECT
        *,
        RANK() OVER (PARTITION BY brand, product_id ORDER BY quantity NULLS LAST) AS quantity_rank,
        AVG(brand_count_quantity) OVER (PARTITION BY product_id, order_id) AS product_id_avg_brand_count_quantity
    FROM enriched_order_items_2
)
-- Ranking and gap features
SELECT
    *,
    DENSE_RANK() OVER (PARTITION BY order_id, product_id ORDER BY brand_count_quantity NULLS LAST) AS brand_count_quantity_dense_rank,
    COUNT(quantity_share) OVER (PARTITION BY order_id) AS order_id_count_quantity_share,
    DENSE_RANK() OVER w_hist AS product_id_avg_brand_count_quantity_dense_rank,
    CUME_DIST() OVER w_part AS quantity_dense_rank_cume_dist
FROM with_windows_order_items
WINDOW w_hist AS (PARTITION BY product_id, order_id ORDER BY product_id_avg_brand_count_quantity DESC NULLS LAST), w_part AS (PARTITION BY order_id, product_id ORDER BY quantity_dense_rank DESC NULLS LAST);
