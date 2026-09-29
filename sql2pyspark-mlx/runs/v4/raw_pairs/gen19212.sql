WITH lines AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        customer_id,
        created_at,
        li_item.product_id,
        li_item.qty,
        li_item.price,
        li_item.promo
    FROM (
        SELECT
            id,
            customer_id,
            created_at,
            EXPLODE(items) AS li_item
        FROM carts
    ) t
),
enriched AS (
    -- Attach customers attributes
    SELECT
        e.*,
        p.email,
        p.name
    FROM lines e
    LEFT JOIN customers p
        ON e.customer_id = p.id
),
cleaned AS (
    -- Basic cleansing and defaults
    SELECT *, TRIM(email) AS clean_email
    FROM enriched
    WHERE created_at >= DATE '2025-03-02'
      AND customer_id IS NOT NULL
),
enriched_carts AS (
    -- Attach customers attributes
    SELECT src.*, b.age
    FROM cleaned src
    INNER JOIN customers b
        ON src.customer_id = b.id
),
with_dims AS (
    -- Attach customers attributes
    SELECT
        a.*,
        p.name AS customer_name,
        p.age AS customer_age
    FROM enriched_carts a
    LEFT JOIN customers p
        ON a.customer_id = p.id
)
-- Aggregate with conditional metrics
SELECT
    DATE_TRUNC('HOUR', created_at) AS hour_window,
    promo,
    DATE_TRUNC('DAY', created_at) AS activity_day,
    ROUND(AVG(qty), 2) AS avg_qty,
    (UNIX_TIMESTAMP(MAX(created_at)) - UNIX_TIMESTAMP(MIN(created_at))) / 60 AS span_minutes,
    PERCENTILE_APPROX(qty, ARRAY(0.25, 0.5, 0.75)) AS qty_quartiles,
    COUNT(*) FILTER (WHERE promo = 'BUNDLE') AS bundle_rows,
    DENSE_RANK() OVER (PARTITION BY DATE_TRUNC('HOUR', created_at) ORDER BY (UNIX_TIMESTAMP(MAX(created_at)) - UNIX_TIMESTAMP(MIN(created_at))) / 60 DESC) AS span_minutes_dense_rank
FROM with_dims
GROUP BY DATE_TRUNC('HOUR', created_at), promo, DATE_TRUNC('DAY', created_at)
HAVING avg_qty > 0;
