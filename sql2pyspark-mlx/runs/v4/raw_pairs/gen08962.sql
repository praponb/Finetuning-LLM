WITH scored AS (
    -- Running and moving aggregates
    SELECT
        *,
        ROW_NUMBER() OVER (ORDER BY unit_price DESC, id) AS seq_no,
        COUNT(quantity) OVER (PARTITION BY product_id) AS product_id_count_quantity,
        AVG(unit_price) OVER (PARTITION BY product_id ORDER BY unit_price, id ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING) AS unit_price_avg_2_prior,
        ROW_NUMBER() OVER (PARTITION BY product_id, order_id ORDER BY quantity DESC, id) AS row_seq
    FROM order_items
),
enriched AS (
    -- Attach products attributes
    SELECT a.*, d.price
    FROM scored a
    LEFT JOIN products d
        ON a.product_id = d.id
),
scored_order_items AS (
    -- Ranking and gap features
    SELECT
        id,
        order_id,
        product_id,
        quantity,
        unit_price,
        seq_no,
        product_id_count_quantity,
        unit_price_avg_2_prior,
        row_seq,
        price,
        NTILE(4) OVER (PARTITION BY order_id, product_id ORDER BY price DESC, id) AS price_bucket,
        COUNT(product_id_count_quantity) OVER w_hist AS order_id_count_product_id_count_quantity,
        CUME_DIST() OVER w_recent AS quantity_cume_dist
    FROM enriched
    WINDOW w_hist AS (PARTITION BY order_id), w_recent AS (PARTITION BY order_id ORDER BY quantity)
),
with_dims AS (
    -- Attach products attributes
    SELECT
        e.*,
        p.product_name,
        p.category,
        p.cost
    FROM scored_order_items e
    LEFT JOIN products p
        ON e.product_id = p.id
),
enriched_order_items AS (
    -- Attach products attributes
    SELECT
        e.*,
        b.category AS product_category,
        b.product_name AS product_product_name,
        b.price AS product_price
    FROM with_dims e
    INNER JOIN products b
        ON e.product_id = b.id
),
joined AS (
    -- Attach orders attributes
    SELECT
        src.*,
        b.discount,
        b.status,
        b.amount
    FROM enriched_order_items src
    LEFT JOIN orders b
        ON src.order_id = b.id
),
with_dims_order_items AS (
    -- Attach orders attributes
    SELECT s.*, d.status AS order_status
    FROM joined s
    LEFT JOIN orders d
        ON s.order_id = d.id
)
-- Native SQL PIVOT
SELECT *
FROM (
    SELECT
        product_id,
        order_status,
        category,
        quantity_cume_dist
    FROM with_dims_order_items
) src
PIVOT (
    MAX(quantity_cume_dist) AS value_sum,
    COUNT(1) AS cnt
    FOR category IN ('Home', 'Fashion')
)
