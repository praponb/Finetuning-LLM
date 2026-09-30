WITH valid AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        order_id,
        product_id,
        quantity,
        unit_price,
        unit_price * quantity AS line_total
    FROM order_items
    WHERE unit_price IS NOT NULL
),
enriched AS (
    -- Attach products attributes
    SELECT
        src.*,
        b.product_name,
        b.brand,
        b.cost
    FROM valid src
    INNER JOIN products b
        ON src.product_id = b.id
),
enriched_order_items AS (
    -- Attach products attributes
    SELECT
        s.*,
        d.product_name AS product_product_name,
        d.launch_date
    FROM enriched s
    LEFT JOIN products d
        ON s.product_id = d.id
),
enriched_order_items_2 AS (
    -- Attach orders attributes
    SELECT e.*, b.discount
    FROM enriched_order_items e
    INNER JOIN orders b
        ON e.order_id = b.id
),
stats AS (
    -- Grouped metrics
    SELECT
        order_id,
        DATE_TRUNC('WEEK', launch_date) AS week_start,
        brand,
        COUNT(DISTINCT order_id) AS distinct_order_ids,
        COUNT_IF(brand = 'Initech') AS initech_events,
        SIZE(COLLECT_SET(brand)) AS num_brands,
        APPROX_PERCENTILE(cost, ARRAY(0.25, 0.5, 0.75)) AS cost_quartiles,
        RANK() OVER (PARTITION BY order_id ORDER BY SIZE(COLLECT_SET(brand)) DESC) AS num_brands_rank
    FROM enriched_order_items_2
    GROUP BY order_id, DATE_TRUNC('WEEK', launch_date), brand
)
SELECT
    order_id,
    week_start,
    brand,
    distinct_order_ids,
    num_brands,
    cost_quartiles
FROM stats
ORDER BY brand;
