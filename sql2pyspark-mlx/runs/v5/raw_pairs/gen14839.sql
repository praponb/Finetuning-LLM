WITH base AS (
    -- Filter the raw rows and derive helper columns
    SELECT *, CAST(quantity AS STRING) AS quantity_num
    FROM order_items
    WHERE product_id IS NOT NULL
),
with_dims AS (
    -- Attach orders attributes
    SELECT
        a.id,
        a.order_id,
        a.unit_price,
        b.discount,
        b.order_date,
        b.amount
    FROM base a
    LEFT JOIN orders b
        ON a.order_id = b.id
),
enriched AS (
    -- Attach orders attributes
    SELECT src.*, p.status
    FROM with_dims src
    INNER JOIN orders p
        ON src.order_id = p.id
),
joined AS (
    -- Attach orders attributes
    SELECT
        e.*,
        b.amount AS order_amount,
        b.order_date AS order_order_date
    FROM enriched e
    LEFT JOIN orders b
        ON e.order_id = b.id
),
enriched_order_items AS (
    -- Attach orders attributes
    SELECT
        a.id,
        a.order_id,
        a.order_date,
        a.order_amount,
        a.order_order_date,
        o.channel,
        o.order_date AS order_order_date_2
    FROM joined a
    INNER JOIN orders o
        ON a.order_id = o.id
),
cleaned AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        order_id,
        order_date,
        order_order_date,
        order_order_date_2,
        CASE WHEN order_amount >= 1470 THEN 'High' WHEN order_amount >= 680 THEN 'Medium' ELSE 'Low' END AS order_amount_level
    FROM enriched_order_items
    WHERE channel IS NOT NULL
      AND channel IS NOT DISTINCT FROM 'web'
)
-- Grouped metrics
SELECT
    DATE_TRUNC('MONTH', order_order_date_2) AS activity_month,
    order_id,
    COUNT(*) AS cnt,
    COUNT(DISTINCT id) AS distinct_ids,
    COUNT(DISTINCT id) AS distinct_ids_2,
    GROUPING_ID() AS grouping_level
FROM cleaned
GROUP BY CUBE (DATE_TRUNC('MONTH', order_order_date_2), order_id)
HAVING COUNT(*) > 1;
