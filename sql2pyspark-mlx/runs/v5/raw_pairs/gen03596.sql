WITH base AS (
    -- Keep recent, valid rows
    SELECT
        id,
        quantity,
        unit_price,
        ROUND(unit_price, 0) AS unit_price_rounded
    FROM order_items
    WHERE order_id = 28
),
base_order_items AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        ROUND(unit_price * 1.07, 2) AS unit_price_adjusted,
        ROUND(unit_price_rounded * 0.85, 2) AS adjusted_unit_price_rounded
    FROM base
    WHERE unit_price <= 110
),
with_windows AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        COUNT(quantity) OVER w_running AS last4_count_quantity,
        MIN(quantity) OVER w_running AS last4_min_quantity,
        AVG(quantity) OVER w_running AS last4_avg_quantity
    FROM base_order_items
    WINDOW w_running AS (ORDER BY unit_price_adjusted, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW)
),
metrics AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        AVG(adjusted_unit_price_rounded) OVER (ORDER BY unit_price_rounded, id ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING) AS adjusted_unit_price_rounded_avg_3_prior,
        NTILE(3) OVER (ORDER BY last4_count_quantity, id) AS last4_count_quantity_bucket,
        LAST_VALUE(last4_avg_quantity) IGNORE NULLS OVER (ORDER BY adjusted_unit_price_rounded, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_last4_avg_quantity
    FROM with_windows
),
valid AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        unit_price,
        unit_price_rounded,
        unit_price_adjusted,
        adjusted_unit_price_rounded,
        last4_count_quantity,
        last4_min_quantity,
        last4_avg_quantity,
        adjusted_unit_price_rounded_avg_3_prior,
        last4_count_quantity * quantity AS last4_count_quantity_times_quantity
    FROM metrics
    WHERE adjusted_unit_price_rounded <= 44
)
SELECT
    id,
    unit_price_rounded,
    adjusted_unit_price_rounded,
    last4_count_quantity,
    last4_avg_quantity,
    adjusted_unit_price_rounded_avg_3_prior,
    last4_count_quantity_times_quantity
FROM valid;
