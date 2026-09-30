WITH filtered AS (
    SELECT
        *,
        FLOOR(quantity) quantity_whole,
        ROUND(quantity * 100, 2) quantity_adjusted
    FROM inventory
    WHERE last_restock >= DATE '2023-02-07'
),
ranked AS (
    SELECT
        id,
        store_id,
        last_restock,
        quantity_whole,
        quantity_adjusted
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY store_id, product_id ORDER BY quantity_whole ASC NULLS FIRST, id ASC) AS rn
        FROM filtered
    ) t
    WHERE rn = 1
),
base AS (
    SELECT
        id,
        store_id,
        quantity_whole,
        quantity_adjusted,
        LAST_DAY(last_restock) last_restock_month_end
    FROM ranked
    WHERE last_restock > DATE '2023-02-15'
),
filtered_inventory AS (
    SELECT *, DATE_TRUNC('MONTH', last_restock_month_end) last_restock_month_end_period
    FROM base
    WHERE quantity_adjusted >= 34
),
gaps AS (
    SELECT *, CASE WHEN last_restock_month_end_period - LAG(last_restock_month_end_period) OVER (PARTITION BY store_id ORDER BY last_restock_month_end_period ASC, id ASC) > INTERVAL 30 DAYS THEN 1 ELSE 0 END AS is_new_burst
    FROM filtered_inventory
),
sessionized AS (
    SELECT *, SUM(is_new_burst) OVER (PARTITION BY store_id ORDER BY last_restock_month_end_period ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_no
    FROM gaps
)
SELECT
    id,
    quantity_adjusted,
    last_restock_month_end,
    is_new_burst,
    session_no
FROM sessionized
ORDER BY session_no ASC NULLS FIRST, is_new_burst ASC
