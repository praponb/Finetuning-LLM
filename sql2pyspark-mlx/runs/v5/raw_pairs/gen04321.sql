WITH ranked AS (
    SELECT
        id,
        quantity,
        last_restock
    FROM inventory
    QUALIFY RANK() OVER (PARTITION BY product_id, store_id ORDER BY quantity DESC NULLS FIRST) = 1
),
filtered AS (
    SELECT
        *,
        DATE_ADD(last_restock, 8) AS last_restock_plus_8d,
        DATEDIFF(CURRENT_DATE(), last_restock) AS days_since_last_restock
    FROM ranked
    WHERE last_restock > DATE '2023-08-27'
),
cleaned AS (
    SELECT
        *,
        DATE_ADD(last_restock_plus_8d, 51) AS last_restock_plus_8d_plus_51d,
        days_since_last_restock * quantity AS days_since_last_restock_times_quantity
    FROM filtered
    WHERE last_restock >= '2023-11-01'
),
valid AS (
    SELECT *, DATE_SUB(last_restock, 15) AS last_restock_minus_15d
    FROM cleaned
    WHERE last_restock_plus_8d BETWEEN '2023-09-30' AND '2024-12-29'
),
scored AS (
    SELECT
        id,
        quantity,
        last_restock,
        last_restock_plus_8d,
        days_since_last_restock,
        last_restock_minus_15d,
        ROW_NUMBER() OVER (ORDER BY days_since_last_restock_times_quantity, id) AS row_seq,
        AVG(days_since_last_restock) OVER (ORDER BY last_restock_plus_8d_plus_51d, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS days_since_last_restock_avg_6
    FROM valid
),
metrics AS (
    SELECT
        *,
        AVG(days_since_last_restock_avg_6) OVER (ORDER BY days_since_last_restock_avg_6, id ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING) AS days_since_last_restock_avg_6_avg_5_prior,
        SUM(days_since_last_restock) OVER w_ordered AS running_days_since_last_restock,
        PERCENT_RANK() OVER w_grp AS row_seq_percent_rank,
        NTILE(5) OVER w_part AS days_since_last_restock_avg_6_bucket,
        LAG(quantity, 1) OVER w_hist AS prev_quantity
    FROM scored
    WINDOW w_ordered AS (ORDER BY row_seq, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW), w_grp AS (ORDER BY row_seq DESC NULLS LAST), w_part AS (ORDER BY days_since_last_restock_avg_6 DESC, id), w_hist AS (ORDER BY last_restock_plus_8d, id)
)
SELECT
    DATE_TRUNC('WEEK', last_restock_minus_15d) AS week_start,
    PERCENTILE_APPROX(days_since_last_restock_avg_6_bucket, 0.95) AS days_since_last_restock_avg_6_bucket_p95,
    PERCENTILE_APPROX(quantity, ARRAY(0.25, 0.5, 0.75)) AS quantity_quartiles
FROM metrics
GROUP BY DATE_TRUNC('WEEK', last_restock_minus_15d)
ORDER BY days_since_last_restock_avg_6_bucket_p95 NULLS FIRST;
