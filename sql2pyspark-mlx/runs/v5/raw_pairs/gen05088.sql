WITH cleaned AS (
    -- Basic cleansing and defaults
    SELECT *, CASE WHEN quantity >= 7 THEN 'High' WHEN quantity >= 4 THEN 'Medium' ELSE 'Low' END AS quantity_tier
    FROM order_items
    WHERE product_id IS NOT NULL
),
metrics AS (
    -- Ranking and gap features
    SELECT
        *,
        SUM(unit_price) OVER w_grp AS cumulative_unit_price,
        FIRST_VALUE(quantity) IGNORE NULLS OVER (ORDER BY quantity ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_quantity,
        ROW_NUMBER() OVER w_grp_2 AS seq_no,
        ROW_NUMBER() OVER (PARTITION BY order_id, product_id ORDER BY unit_price ASC, id ASC) AS seq_no_2
    FROM cleaned
    WINDOW w_grp AS (ORDER BY unit_price ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW), w_grp_2 AS (PARTITION BY order_id ORDER BY unit_price ASC, id ASC)
),
kept_keys AS (
    -- Set intersection on keys
    SELECT product_id, order_id
    FROM metrics
    INTERSECT
    SELECT product_id, order_id
    FROM order_items
    WHERE order_id IN (5, 28)
),
kept_rows AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM metrics s
    INNER JOIN kept_keys k ON s.product_id = k.product_id AND s.order_id = k.order_id
),
metrics_order_items AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        ROUND(seq_no / SUM(seq_no) OVER (PARTITION BY order_id), 4) AS seq_no_share,
        CUME_DIST() OVER w_hist AS seq_no_2_cume_dist,
        ROUND(first_known_quantity / SUM(first_known_quantity) OVER (PARTITION BY product_id, order_id), 4) AS first_known_quantity_share,
        PERCENT_RANK() OVER (ORDER BY seq_no DESC NULLS LAST) AS seq_no_percent_rank
    FROM kept_rows
    WHERE order_id IN (13, 21, 22, 36)
    WINDOW w_hist AS (PARTITION BY order_id ORDER BY seq_no_2 DESC)
)
-- Join a dimension and aggregate
SELECT
    p.brand,
    COUNT(*) AS num_rows,
    AVG(src.seq_no) AS avg_seq_no
FROM metrics_order_items src
LEFT JOIN products p ON src.product_id = p.id
GROUP BY p.brand
ORDER BY brand DESC
