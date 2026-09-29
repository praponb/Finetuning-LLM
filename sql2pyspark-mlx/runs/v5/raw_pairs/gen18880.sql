WITH ranked AS (
    -- Keep one row per key (latest first)
    SELECT *
    FROM order_items
    QUALIFY ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY unit_price NULLS FIRST, id) = 1
),
remaining_keys AS (
    -- Set difference on keys
    SELECT order_id, product_id
    FROM ranked
    EXCEPT DISTINCT
    SELECT order_id, product_id
    FROM order_items
    WHERE product_id IN (2, 21)
),
reconciled AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM ranked s
    JOIN remaining_keys k ON s.order_id = k.order_id AND s.product_id = k.product_id
),
with_dims AS (
    -- Attach orders attributes
    SELECT
        s.*,
        p.order_date,
        p.status,
        p.channel
    FROM reconciled s
    JOIN orders p
        ON s.order_id = p.id
),
metrics AS (
    -- Ranking and gap features
    SELECT
        product_id,
        quantity,
        unit_price,
        order_date,
        channel,
        CUME_DIST() OVER (PARTITION BY channel ORDER BY unit_price NULLS FIRST) unit_price_cume_dist,
        CORR(unit_price, quantity) OVER (PARTITION BY status) corr_unit_price_quantity,
        RANK() OVER (ORDER BY quantity NULLS LAST) quantity_rank,
        ROUND(quantity / SUM(quantity) OVER (PARTITION BY channel), 4) quantity_share
    FROM with_dims
),
scored AS (
    -- Running and moving aggregates
    SELECT
        quantity,
        order_date,
        unit_price_cume_dist,
        corr_unit_price_quantity,
        quantity_share,
        MAX(corr_unit_price_quantity) OVER (PARTITION BY product_id) product_id_max_corr_unit_price_quantity,
        CUME_DIST() OVER (PARTITION BY product_id ORDER BY unit_price DESC NULLS LAST) unit_price_cume_dist_2
    FROM metrics
),
daily AS (
    -- Aggregate with conditional metrics
    SELECT
        DATE_TRUNC('WEEK', order_date) AS week_start,
        COUNT(*) AS event_count,
        MAX(unit_price_cume_dist) AS max_unit_price_cume_dist,
        PERCENTILE_APPROX(quantity_share, ARRAY(0.25, 0.5, 0.75)) AS quantity_share_quartiles
    FROM scored
    GROUP BY 1
    HAVING max_unit_price_cume_dist >= 0
)
SELECT
    week_start,
    event_count,
    quantity_share_quartiles
FROM daily;
