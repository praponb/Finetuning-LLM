WITH filtered AS (
    SELECT *, LEAST(unit_price, quantity) least_unit_price_quantity
    FROM order_items
    WHERE product_id IS NOT NULL
      AND unit_price >= 300
),
cleaned AS (
    SELECT *, quantity - least_unit_price_quantity quantity_minus_least_unit_price_quantity
    FROM filtered
    WHERE product_id IS NOT NULL
),
metrics AS (
    SELECT
        *,
        ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY unit_price, id) row_seq,
        ROW_NUMBER() OVER (PARTITION BY product_id, order_id ORDER BY unit_price, id) row_seq_2,
        MIN(quantity_minus_least_unit_price_quantity) OVER (PARTITION BY product_id) product_id_min_quantity_minus_least_unit_price_quantity
    FROM cleaned
),
with_windows AS (
    SELECT
        id,
        order_id,
        product_id,
        unit_price,
        row_seq,
        row_seq_2,
        LAST_VALUE(product_id_min_quantity_minus_least_unit_price_quantity, TRUE) OVER (ORDER BY row_seq_2, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) last_known_product_id_min_quantity_minus_least_unit_price_quantity,
        AVG(quantity) OVER (ORDER BY least_unit_price_quantity, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) quantity_avg_2,
        CORR(quantity, unit_price) OVER (PARTITION BY order_id) corr_quantity_unit_price
    FROM metrics
),
latest AS (
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY row_seq_2 NULLS LAST, id) AS rank_in_group
        FROM with_windows
    ) x
    WHERE rank_in_group = 1
),
latest_order_items AS (
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY product_id ORDER BY quantity_avg_2 DESC NULLS LAST, id) AS latest_rank
        FROM latest
    ) ranked
    WHERE latest_rank = 1
)
SELECT
    product_id,
    MIN(corr_quantity_unit_price) AS min_corr_quantity_unit_price,
    AVG(unit_price) AS avg_unit_price,
    PERCENTILE_APPROX(row_seq_2, 0.9) AS row_seq_2_p90
FROM latest_order_items
GROUP BY product_id
HAVING AVG(unit_price) > 0
ORDER BY avg_unit_price DESC NULLS LAST;
