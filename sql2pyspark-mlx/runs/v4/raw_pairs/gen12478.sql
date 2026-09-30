WITH filtered AS (
    SELECT
        *,
        DATE_TRUNC('year', last_restock) AS last_restock_period,
        DATE_FORMAT(last_restock, 'yyyy-MM-dd') AS last_restock_label
    FROM inventory
    WHERE last_restock >= DATE '2024-04-01'
),
gaps AS (
    SELECT *, CASE WHEN LAG(last_restock_period) OVER (PARTITION BY product_id ORDER BY last_restock_period, id) IS NULL
                 OR last_restock_period > LAG(last_restock_period) OVER (PARTITION BY product_id ORDER BY last_restock_period, id) + INTERVAL '7' DAY THEN 1 ELSE 0 END AS is_new_burst
    FROM filtered
),
sessionized AS (
    SELECT *, SUM(is_new_burst) OVER (PARTITION BY product_id ORDER BY last_restock_period, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS burst_id
    FROM gaps
),
scored AS (
    SELECT
        *,
        NTILE(4) OVER (PARTITION BY product_id, store_id ORDER BY is_new_burst, id) AS is_new_burst_bucket,
        CUME_DIST() OVER w_running AS is_new_burst_cume_dist
    FROM sessionized
    WINDOW w_running AS (ORDER BY is_new_burst DESC NULLS LAST)
),
ranked AS (
    SELECT *
    FROM scored
    QUALIFY ROW_NUMBER() OVER (PARTITION BY store_id ORDER BY burst_id, id) = 1
),
deduped AS (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY store_id ORDER BY is_new_burst_bucket NULLS FIRST, id) AS rn
    FROM ranked
    QUALIFY rn = 1
),
metrics AS (
    SELECT
        *,
        AVG(is_new_burst) OVER (PARTITION BY store_id ORDER BY CAST(last_restock_period AS BIGINT) RANGE BETWEEN 604800 PRECEDING AND CURRENT ROW) AS is_new_burst_avg_7d,
        SUM(quantity) OVER (PARTITION BY store_id ORDER BY is_new_burst, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_quantity,
        SUM(burst_id) OVER (PARTITION BY store_id ORDER BY last_restock, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_burst_id,
        SUM(is_new_burst_bucket) OVER (ORDER BY burst_id, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_is_new_burst_bucket
    FROM deduped
)
SELECT
    running_burst_id,
    CASE WHEN cumulative_quantity >= 1123 THEN 'PREMIUM' ELSE 'STANDARD' END AS cumulative_quantity_tier,
    COUNT(*) AS cnt,
    AVG(cumulative_is_new_burst_bucket) AS avg_cumulative_is_new_burst_bucket,
    PERCENTILE_APPROX(running_burst_id, 0.95) AS running_burst_id_p95,
    DENSE_RANK() OVER (PARTITION BY running_burst_id ORDER BY AVG(cumulative_is_new_burst_bucket) DESC) AS avg_cumulative_is_new_burst_bucket_dense_rank
FROM metrics
GROUP BY running_burst_id, CASE WHEN cumulative_quantity >= 1123 THEN 'PREMIUM' ELSE 'STANDARD' END
ORDER BY cumulative_quantity_tier DESC NULLS LAST
