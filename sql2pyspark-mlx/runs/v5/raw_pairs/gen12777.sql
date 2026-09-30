WITH latest AS (
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY product_id ORDER BY unit_price DESC NULLS FIRST, id) AS rank_in_group
        FROM order_items
    ) x
    WHERE rank_in_group = 1
),
with_dims AS (
    SELECT a.*, b.order_date
    FROM latest a
    LEFT JOIN orders b
        ON a.order_id = b.id
),
with_dims_order_items AS (
    SELECT
        src.*,
        o.order_date AS order_order_date,
        o.amount
    FROM with_dims src
    LEFT JOIN orders o
        ON src.order_id = o.id
),
remaining_keys AS (
    SELECT id
    FROM with_dims_order_items
    EXCEPT DISTINCT
    SELECT id
    FROM order_items
    WHERE product_id IN (15, 38)
),
reconciled AS (
    SELECT s.*
    FROM with_dims_order_items s
    JOIN remaining_keys k ON s.id = k.id
),
scored AS (
    SELECT
        id,
        order_id,
        product_id,
        unit_price,
        ROUND(unit_price / SUM(unit_price) OVER (PARTITION BY order_id), 4) AS unit_price_share,
        COUNT(quantity) OVER (PARTITION BY order_id) AS order_id_count_quantity,
        AVG(quantity) OVER (PARTITION BY product_id, order_id) AS product_id_avg_quantity,
        PERCENT_RANK() OVER (ORDER BY amount DESC NULLS FIRST) AS amount_percent_rank,
        RANK() OVER (PARTITION BY order_id ORDER BY amount DESC) AS amount_rank
    FROM reconciled
    WHERE order_date <= '2024-02-01'
),
enriched AS (
    SELECT
        *,
        DENSE_RANK() OVER (ORDER BY unit_price_share DESC NULLS FIRST) AS unit_price_share_dense_rank,
        MIN(product_id_avg_quantity) OVER (PARTITION BY product_id) AS product_id_min_product_id_avg_quantity
    FROM scored
),
scored_order_items AS (
    SELECT
        order_id,
        product_id,
        unit_price,
        unit_price_share,
        order_id_count_quantity,
        product_id_avg_quantity,
        unit_price_share_dense_rank,
        STDDEV_SAMP(unit_price_share_dense_rank) OVER w_ordered AS product_id_stddev_samp_unit_price_share_dense_rank,
        DENSE_RANK() OVER w_ordered_2 AS amount_percent_rank_dense_rank
    FROM enriched
    WINDOW w_ordered AS (PARTITION BY product_id), w_ordered_2 AS (ORDER BY amount_percent_rank DESC NULLS LAST)
)
SELECT
    order_id,
    product_id,
    unit_price_share,
    product_id_avg_quantity,
    unit_price_share_dense_rank,
    amount_percent_rank_dense_rank
FROM scored_order_items
ORDER BY order_id
