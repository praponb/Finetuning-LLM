WITH base AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        unit_price * quantity AS line_total,
        ROUND(unit_price, 2) AS unit_price_rounded
    FROM order_items
    WHERE product_id IS NOT NULL
),
kept_keys AS (
    -- Set intersection on keys
    SELECT product_id, order_id
    FROM base
    INTERSECT ALL
    SELECT product_id, order_id
    FROM order_items
    WHERE order_id IN (11, 33)
),
reconciled AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM base s
    JOIN kept_keys k ON s.product_id = k.product_id AND s.order_id = k.order_id
),
enriched AS (
    -- Attach orders attributes
    SELECT
        s.*,
        o.amount,
        o.order_date,
        o.channel
    FROM reconciled s
    LEFT JOIN orders o
        ON s.order_id = o.id
    WHERE o.customer_id = 27
),
joined AS (
    -- Attach orders attributes
    SELECT
        a.id,
        a.order_id,
        a.product_id,
        a.unit_price,
        a.line_total,
        a.amount,
        a.order_date,
        a.channel,
        o.status,
        o.discount
    FROM enriched a
    JOIN orders o
        ON a.order_id = o.id
),
enriched_order_items AS (
    -- Ranking and gap features
    SELECT *, AVG(discount) OVER w_part AS product_id_avg_discount
    FROM joined
    WINDOW w_part AS (PARTITION BY product_id, status)
),
metrics AS (
    -- Window metrics over several different partitions
    SELECT
        order_id,
        product_id,
        unit_price,
        line_total,
        order_date,
        product_id_avg_discount,
        CUME_DIST() OVER (ORDER BY amount) AS amount_cume_dist,
        ROUND(unit_price / SUM(unit_price) OVER (PARTITION BY channel, product_id), 4) AS unit_price_share
    FROM enriched_order_items
),
enriched_order_items_2 AS (
    -- Attach products attributes
    SELECT
        a.product_id,
        a.unit_price,
        a.amount_cume_dist,
        d.product_name,
        d.cost
    FROM metrics a
    LEFT JOIN products d
        ON a.product_id = d.id
)
SELECT
    s.product_id,
    s.unit_price,
    s.amount_cume_dist,
    s.product_name,
    s.cost,
    (SELECT MAX(x.quantity) FROM inventory x WHERE x.product_id = s.product_id) AS max_inventory_quantity
FROM enriched_order_items_2 s
ORDER BY amount_cume_dist NULLS FIRST, product_id
