WITH filtered AS (
    SELECT
        id,
        product_id,
        quantity,
        CASE WHEN quantity >= 240 THEN 'High' WHEN quantity >= 107 THEN 'Medium' ELSE 'Low' END AS quantity_tier
    FROM inventory
    WHERE product_id IN (2, 3)
),
base AS (
    SELECT *, FLOOR(quantity) AS quantity_whole
    FROM filtered
    WHERE quantity <= 128
),
metrics AS (
    SELECT
        id,
        product_id,
        quantity,
        quantity_whole,
        MIN(quantity) OVER (PARTITION BY product_id ORDER BY quantity_whole, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS last7_min_quantity,
        AVG(quantity) OVER (PARTITION BY product_id ORDER BY quantity_whole, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS last7_avg_quantity,
        MAX(quantity) OVER (PARTITION BY product_id ORDER BY quantity_whole, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS last7_max_quantity,
        LAST_VALUE(quantity_whole, TRUE) OVER (PARTITION BY product_id ORDER BY quantity, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_quantity_whole
    FROM base
),
latest AS (
    SELECT
        id,
        product_id,
        quantity,
        quantity_whole,
        last7_min_quantity,
        last7_avg_quantity,
        last7_max_quantity
    FROM metrics
    QUALIFY RANK() OVER (PARTITION BY product_id ORDER BY quantity DESC NULLS FIRST) = 1
),
cleaned AS (
    SELECT
        id,
        quantity,
        last7_min_quantity,
        last7_max_quantity,
        CASE WHEN quantity_whole >= 89 THEN 'High' WHEN quantity_whole >= 30 THEN 'Medium' ELSE 'Low' END AS quantity_whole_band
    FROM latest
    WHERE product_id = 9
),
with_windows AS (
    SELECT
        *,
        ROW_NUMBER() OVER (ORDER BY last7_max_quantity, id) AS event_seq,
        ROW_NUMBER() OVER (ORDER BY last7_min_quantity, id) AS seq_no,
        FIRST_VALUE(last7_min_quantity, TRUE) OVER w_hist AS first_known_last7_min_quantity,
        SUM(quantity) OVER w_hist AS cumulative_quantity
    FROM cleaned
    WHERE quantity <= 183
    WINDOW w_hist AS (ORDER BY quantity, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
)
SELECT
    id,
    last7_max_quantity,
    quantity_whole_band,
    seq_no,
    first_known_last7_min_quantity,
    cumulative_quantity
FROM with_windows
