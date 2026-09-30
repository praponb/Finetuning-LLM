WITH cleaned AS (
    SELECT
        id,
        store_id,
        quantity,
        DATE_FORMAT(last_restock, 'yyyy-MM') AS last_restock_label
    FROM inventory
    WHERE product_id IN (4, 6, 7)
),
scored AS (
    SELECT
        id,
        quantity,
        AVG(quantity) OVER w_hist AS last4_avg_quantity,
        SUM(quantity) OVER w_hist AS last4_sum_quantity,
        COUNT(quantity) OVER w_hist AS last4_count_quantity
    FROM cleaned
    WINDOW w_hist AS (PARTITION BY store_id ORDER BY quantity, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW)
),
valid AS (
    SELECT
        id,
        quantity,
        last4_avg_quantity,
        last4_sum_quantity,
        last4_count_quantity,
        LEAST(last4_avg_quantity, quantity) AS least_last4_avg_quantity_quantity
    FROM scored
    WHERE last4_sum_quantity IS NOT NULL
),
with_windows AS (
    SELECT
        *,
        LAST_VALUE(least_last4_avg_quantity_quantity) IGNORE NULLS OVER w_hist AS last_known_least_last4_avg_quantity_quantity,
        SUM(least_last4_avg_quantity_quantity) OVER (ORDER BY least_last4_avg_quantity_quantity, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_least_last4_avg_quantity_quantity
    FROM valid
    WHERE quantity >= 113
    WINDOW w_hist AS (ORDER BY quantity, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
),
with_windows_inventory AS (
    SELECT
        *,
        SUM(running_least_last4_avg_quantity_quantity) OVER w_recent AS last3_sum_running_least_last4_avg_quantity_quantity,
        COUNT(running_least_last4_avg_quantity_quantity) OVER w_recent AS last3_count_running_least_last4_avg_quantity_quantity,
        MIN(running_least_last4_avg_quantity_quantity) OVER w_recent AS last3_min_running_least_last4_avg_quantity_quantity,
        AVG(last4_avg_quantity) OVER (ORDER BY last4_sum_quantity, id ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING) AS last4_avg_quantity_avg_6_prior
    FROM with_windows
    WINDOW w_recent AS (ORDER BY running_least_last4_avg_quantity_quantity, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW)
)
SELECT
    id,
    quantity,
    last4_avg_quantity,
    last4_sum_quantity,
    last4_count_quantity,
    least_last4_avg_quantity_quantity,
    last_known_least_last4_avg_quantity_quantity,
    running_least_last4_avg_quantity_quantity,
    last3_sum_running_least_last4_avg_quantity_quantity
FROM with_windows_inventory
ORDER BY id NULLS FIRST
