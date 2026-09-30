WITH base AS (
    SELECT
        id,
        order_id,
        product_id,
        quantity,
        unit_price,
        COALESCE(unit_price, 0) AS unit_price_filled
    FROM order_items
    WHERE product_id IS NOT NULL
),
valid AS (
    SELECT *, COALESCE(unit_price, 0) AS unit_price_filled_2
    FROM base
    WHERE product_id IN (12, 21, 24)
),
enriched AS (
    SELECT
        *,
        ROW_NUMBER() OVER (PARTITION BY product_id ORDER BY unit_price DESC, id) AS row_seq,
        CORR(unit_price, quantity) OVER (PARTITION BY product_id) AS corr_unit_price_quantity,
        AVG(quantity) OVER (ORDER BY unit_price_filled, id ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING) AS quantity_avg_2_prior,
        LAG(unit_price, 2) OVER (PARTITION BY product_id ORDER BY quantity, id) AS prev_unit_price,
        CUME_DIST() OVER (PARTITION BY product_id ORDER BY unit_price_filled_2 DESC NULLS LAST) AS unit_price_filled_2_cume_dist
    FROM valid
),
metrics AS (
    SELECT
        *,
        LAST_VALUE(unit_price_filled_2) IGNORE NULLS OVER w_recent AS last_known_unit_price_filled_2,
        AVG(quantity) OVER (PARTITION BY order_id) AS order_id_avg_quantity
    FROM enriched
    WINDOW w_recent AS (PARTITION BY order_id ORDER BY unit_price_filled_2, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
),
matched_keys AS (
    SELECT id, order_id
    FROM metrics
    INTERSECT
    SELECT id, order_id
    FROM order_items
    WHERE product_id IS NULL
),
verified AS (
    SELECT s.*
    FROM metrics s
    INNER JOIN matched_keys k ON s.id = k.id AND s.order_id = k.order_id
)
SELECT
    CASE WHEN quantity_avg_2_prior >= 9 THEN 'HIGH' ELSE 'LOW' END AS quantity_avg_2_prior_tier,
    product_id,
    SUM(quantity) AS total_quantity,
    BIT_OR(quantity) AS quantity_bit_or,
    APPROX_PERCENTILE(last_known_unit_price_filled_2, ARRAY(0.25, 0.5, 0.75)) AS last_known_unit_price_filled_2_quartiles
FROM verified
GROUP BY CASE WHEN quantity_avg_2_prior >= 9 THEN 'HIGH' ELSE 'LOW' END, product_id
HAVING SUM(quantity) > 0
ORDER BY product_id DESC
