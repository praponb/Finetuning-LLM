WITH valid AS (
    SELECT *, quantity + unit_price AS line_total
    FROM order_items
    WHERE order_id IN (1, 12, 36, 38)
),
with_dims AS (
    SELECT
        a.id,
        a.order_id,
        a.product_id,
        a.quantity,
        a.unit_price,
        a.line_total,
        b.channel,
        b.status
    FROM valid a
    INNER JOIN orders b
        ON a.order_id = b.id
    WHERE b.amount < 490
),
matched_keys AS (
    SELECT order_id, product_id
    FROM with_dims
    INTERSECT ALL
    SELECT order_id, product_id
    FROM order_items
    WHERE quantity < 6
),
reconciled AS (
    SELECT s.*
    FROM with_dims s
    INNER JOIN matched_keys k ON s.order_id = k.order_id AND s.product_id = k.product_id
),
with_dims_order_items AS (
    SELECT
        s.order_id,
        s.product_id,
        s.quantity,
        s.unit_price,
        s.channel,
        p.discount,
        p.amount,
        p.order_date
    FROM reconciled s
    LEFT JOIN orders p
        ON s.order_id = p.id
),
enriched AS (
    SELECT
        s.*,
        b.channel AS order_channel,
        b.shipping_fee
    FROM with_dims_order_items s
    LEFT JOIN orders b
        ON s.order_id = b.id
),
scored AS (
    SELECT
        order_id,
        unit_price,
        discount,
        order_channel,
        shipping_fee,
        RANK() OVER (ORDER BY quantity DESC NULLS LAST) AS quantity_rank,
        CUME_DIST() OVER (PARTITION BY product_id ORDER BY quantity DESC NULLS FIRST) AS quantity_cume_dist,
        DENSE_RANK() OVER (ORDER BY unit_price DESC NULLS FIRST) AS unit_price_dense_rank,
        DENSE_RANK() OVER (PARTITION BY order_channel ORDER BY amount NULLS LAST) AS amount_dense_rank,
        DENSE_RANK() OVER (PARTITION BY order_channel ORDER BY shipping_fee DESC NULLS LAST) AS shipping_fee_dense_rank
    FROM enriched
),
with_dims_order_items_2 AS (
    SELECT src.*, b.status
    FROM scored src
    LEFT JOIN orders b
        ON src.order_id = b.id
)
SELECT *
FROM (
    SELECT
        order_id,
        quantity_cume_dist,
        unit_price
    FROM with_dims_order_items_2
) src
UNPIVOT (
    val FOR measure IN (quantity_cume_dist, unit_price)
)
