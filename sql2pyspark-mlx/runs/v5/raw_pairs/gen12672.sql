WITH base AS (
    SELECT
        *,
        ROUND(unit_price, 0) AS unit_price_rounded,
        quantity * unit_price AS line_total
    FROM order_items
    WHERE product_id IS NOT NULL
),
scored AS (
    SELECT
        id,
        product_id,
        quantity,
        line_total,
        AVG(unit_price) OVER w_grp AS running_avg_unit_price,
        MAX(unit_price) OVER w_grp AS running_max_unit_price,
        SUM(unit_price) OVER w_grp AS running_sum_unit_price
    FROM base
    WINDOW w_grp AS (PARTITION BY order_id, product_id ORDER BY line_total, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
),
cleaned AS (
    SELECT
        id,
        quantity,
        line_total,
        running_avg_unit_price,
        running_max_unit_price,
        running_sum_unit_price,
        LEAST(running_sum_unit_price, running_avg_unit_price) AS least_running_sum_unit_price_running_avg_unit_price
    FROM scored
    WHERE running_max_unit_price <= 280
),
valid AS (
    SELECT *, ROUND(running_sum_unit_price, 1) AS running_sum_unit_price_rounded
    FROM cleaned
    WHERE quantity BETWEEN 6 AND 7
),
filtered AS (
    SELECT
        id,
        quantity,
        line_total,
        running_avg_unit_price,
        running_max_unit_price,
        running_sum_unit_price,
        least_running_sum_unit_price_running_avg_unit_price,
        running_sum_unit_price_rounded,
        running_sum_unit_price_rounded + running_avg_unit_price AS running_sum_unit_price_rounded_plus_running_avg_unit_price
    FROM valid
    WHERE least_running_sum_unit_price_running_avg_unit_price IS NOT NULL
)
SELECT
    id,
    quantity,
    running_avg_unit_price,
    running_max_unit_price,
    running_sum_unit_price,
    least_running_sum_unit_price_running_avg_unit_price,
    running_sum_unit_price_rounded,
    running_sum_unit_price_rounded_plus_running_avg_unit_price
FROM filtered
ORDER BY quantity DESC
