WITH valid AS (
    SELECT *, YEAR(last_restock) last_restock_year
    FROM inventory
    WHERE last_restock >= DATE '2023-07-27'
),
metrics AS (
    SELECT
        id,
        store_id,
        last_restock,
        LAG(last_restock_year) OVER w_hist prev_last_restock_year,
        LEAD(last_restock_year) OVER w_hist next_last_restock_year,
        LEAD(quantity) OVER w_hist next_quantity,
        ROW_NUMBER() OVER w_hist event_seq,
        SUM(last_restock_year) OVER w_running running_last_restock_year
    FROM valid
    WHERE store_id IN (1, 3, 5)
    WINDOW w_hist AS (PARTITION BY product_id, store_id ORDER BY last_restock, id), w_running AS (ORDER BY last_restock, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
),
ranked AS (
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY store_id ORDER BY next_last_restock_year DESC NULLS LAST, id) AS latest_rank
        FROM metrics
    ) ranked
    WHERE latest_rank = 1
),
filtered AS (
    SELECT
        id,
        last_restock,
        prev_last_restock_year,
        next_last_restock_year,
        running_last_restock_year,
        running_last_restock_year + next_last_restock_year running_last_restock_year_plus_next_last_restock_year
    FROM ranked
    WHERE last_restock > DATE '2023-04-14'
)
SELECT
    CASE WHEN prev_last_restock_year >= 88 THEN 'PREMIUM' ELSE 'STANDARD' END AS prev_last_restock_year_tier,
    CASE WHEN running_last_restock_year >= 445 THEN 'LARGE' ELSE 'SMALL' END AS running_last_restock_year_tier,
    SUM(running_last_restock_year_plus_next_last_restock_year) AS total_running_last_restock_year_plus_next_last_restock_year,
    COUNT(DISTINCT id) AS distinct_ids,
    MIN(running_last_restock_year_plus_next_last_restock_year) AS min_running_last_restock_year_plus_next_last_restock_year,
    AVG(running_last_restock_year_plus_next_last_restock_year) AS avg_running_last_restock_year_plus_next_last_restock_year,
    COUNT(DISTINCT id) AS distinct_ids_2
FROM filtered
GROUP BY GROUPING SETS ((CASE WHEN prev_last_restock_year >= 88 THEN 'PREMIUM' ELSE 'STANDARD' END, CASE WHEN running_last_restock_year >= 445 THEN 'LARGE' ELSE 'SMALL' END), (CASE WHEN prev_last_restock_year >= 88 THEN 'PREMIUM' ELSE 'STANDARD' END))
