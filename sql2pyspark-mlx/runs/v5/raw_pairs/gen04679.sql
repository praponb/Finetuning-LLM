WITH enriched AS (
    SELECT
        *,
        CORR(quantity, unit_price) OVER (PARTITION BY order_id, product_id) AS corr_quantity_unit_price,
        STDDEV_SAMP(quantity) OVER (PARTITION BY order_id, product_id) AS order_id_stddev_samp_quantity
    FROM order_items
),
cleaned AS (
    SELECT
        id,
        order_id,
        product_id,
        corr_quantity_unit_price,
        quantity + unit_price AS line_total
    FROM enriched
    WHERE product_id IS NOT NULL
),
enriched_order_items AS (
    SELECT
        e.id,
        e.order_id,
        e.corr_quantity_unit_price,
        e.line_total,
        b.status
    FROM cleaned e
    LEFT JOIN orders b
        ON e.order_id = b.id
),
filtered AS (
    SELECT
        id,
        order_id,
        corr_quantity_unit_price,
        status,
        ROUND(line_total, 2) AS line_total_rounded,
        ABS(corr_quantity_unit_price) AS abs_corr_quantity_unit_price
    FROM enriched_order_items
    WHERE status = 'SHIPPED'
),
scored AS (
    SELECT
        id,
        order_id,
        corr_quantity_unit_price,
        status,
        line_total_rounded,
        abs_corr_quantity_unit_price,
        AVG(abs_corr_quantity_unit_price) OVER (PARTITION BY status, order_id ORDER BY corr_quantity_unit_price, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS last3_avg_abs_corr_quantity_unit_price,
        MAX(abs_corr_quantity_unit_price) OVER (PARTITION BY status, order_id ORDER BY corr_quantity_unit_price, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS last3_max_abs_corr_quantity_unit_price,
        SUM(abs_corr_quantity_unit_price) OVER (PARTITION BY status, order_id ORDER BY corr_quantity_unit_price, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS last3_sum_abs_corr_quantity_unit_price,
        NTILE(5) OVER (PARTITION BY status ORDER BY corr_quantity_unit_price DESC, id) AS corr_quantity_unit_price_bucket
    FROM filtered
),
remaining_keys AS (
    SELECT order_id, id
    FROM scored
    EXCEPT DISTINCT
    SELECT order_id, id
    FROM order_items
    WHERE order_id IN (13, 16)
),
verified AS (
    SELECT s.*
    FROM scored s
    INNER JOIN remaining_keys k ON s.order_id = k.order_id AND s.id = k.id
)
SELECT *
FROM (
    SELECT
        status,
        last3_sum_abs_corr_quantity_unit_price,
        last3_max_abs_corr_quantity_unit_price
    FROM verified
) src
UNPIVOT (
    val FOR metric IN (last3_sum_abs_corr_quantity_unit_price, last3_max_abs_corr_quantity_unit_price)
) u
