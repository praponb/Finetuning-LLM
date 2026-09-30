WITH filtered AS (
    -- Filter the raw rows and derive helper columns
    SELECT *, ROUND(unit_price / quantity, 2) AS line_total
    FROM order_items
    WHERE order_id IN (5, 21, 32)
),
base AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        order_id,
        product_id,
        quantity,
        unit_price,
        line_total,
        GREATEST(line_total, quantity) AS greatest_line_total_quantity
    FROM filtered
    WHERE quantity BETWEEN 4 AND 7
),
enriched AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        NTILE(3) OVER (PARTITION BY order_id, product_id ORDER BY greatest_line_total_quantity DESC, id) AS greatest_line_total_quantity_bucket,
        AVG(line_total) OVER w_grp AS line_total_avg_6
    FROM base
    WINDOW w_grp AS (PARTITION BY product_id ORDER BY greatest_line_total_quantity, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW)
),
enriched_order_items AS (
    -- Attach orders attributes
    SELECT
        src.*,
        p.amount,
        p.order_date,
        p.status
    FROM enriched src
    LEFT JOIN orders p
        ON src.order_id = p.id
),
daily AS (
    -- Aggregate with conditional metrics
    SELECT
        status,
        SIZE(COLLECT_SET(status)) AS num_status,
        ROUND(AVG(quantity), 2) AS avg_quantity,
        ROUND(SUM(unit_price) / NULLIF(COUNT(CASE WHEN status = 'RETURNED' THEN 1 END), 0), 2) AS unit_price_per_returned
    FROM enriched_order_items
    GROUP BY 1
    HAVING ROUND(AVG(quantity), 2) >= 0
)
SELECT
    status,
    num_status,
    avg_quantity,
    unit_price_per_returned
FROM daily
ORDER BY unit_price_per_returned DESC
