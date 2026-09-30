WITH valid AS (
    -- Basic cleansing and defaults
    SELECT *, CAST(unit_price AS INT) AS unit_price_int
    FROM order_items
    WHERE product_id IN (24, 25, 27)
),
enriched AS (
    -- Ranking and gap features
    SELECT
        *,
        NTILE(10) OVER (PARTITION BY order_id, product_id ORDER BY unit_price DESC, id) AS unit_price_bucket,
        AVG(quantity) OVER (PARTITION BY order_id ORDER BY quantity, id ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING) AS quantity_avg_6_prior,
        FIRST_VALUE(unit_price_int, TRUE) OVER (PARTITION BY order_id ORDER BY unit_price, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_unit_price_int,
        FIRST_VALUE(unit_price) IGNORE NULLS OVER w_running AS first_known_unit_price
    FROM valid
    WINDOW w_running AS (ORDER BY quantity, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
),
with_dims AS (
    -- Attach products attributes
    SELECT
        src.id,
        src.order_id,
        src.product_id,
        src.unit_price,
        src.unit_price_bucket,
        src.quantity_avg_6_prior,
        b.cost,
        b.brand
    FROM enriched src
    INNER JOIN products b
        ON src.product_id = b.id
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        *,
        ROUND(unit_price / SUM(unit_price) OVER (PARTITION BY brand), 4) AS unit_price_share,
        LAG(unit_price_bucket, 1) OVER (PARTITION BY brand ORDER BY cost, id) AS prev_unit_price_bucket
    FROM with_dims
),
metrics AS (
    -- Running and moving aggregates
    SELECT
        id,
        order_id,
        unit_price_bucket,
        brand,
        unit_price_share,
        AVG(cost) OVER w_hist AS cost_avg_6,
        ROW_NUMBER() OVER (PARTITION BY brand ORDER BY unit_price_share, id) AS event_seq
    FROM with_windows
    WHERE unit_price_share < 0.64
    WINDOW w_hist AS (PARTITION BY order_id ORDER BY quantity_avg_6_prior, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW)
),
ranked AS (
    -- Keep one row per key (latest first)
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY cost_avg_6 DESC NULLS FIRST, id) AS row_num
        FROM metrics
    ) ranked
    WHERE row_num = 1
),
deduped AS (
    -- Keep one row per key (latest first)
    SELECT
        id,
        order_id,
        brand,
        cost_avg_6,
        event_seq,
        row_num
    FROM (
        SELECT *, RANK() OVER (PARTITION BY brand, order_id ORDER BY unit_price_bucket DESC NULLS LAST) AS row_num_2
        FROM ranked
    ) t
    WHERE row_num_2 = 1
)
SELECT
    s.id,
    s.order_id,
    s.cost_avg_6,
    s.event_seq,
    (SELECT AVG(o2.unit_price) FROM order_items o2 WHERE o2.order_id = s.order_id) AS avg_order_item_unit_price
FROM deduped s
ORDER BY cost_avg_6 NULLS LAST
