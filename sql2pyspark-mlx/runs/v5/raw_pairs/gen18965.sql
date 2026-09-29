WITH base AS (
    SELECT
        id,
        product_id,
        units,
        DATE_ADD(sale_date, 84) AS sale_date_plus_84d
    FROM store_sales
    WHERE sale_date > DATE '2023-10-03'
),
enriched AS (
    SELECT
        id,
        product_id,
        units,
        sale_date_plus_84d,
        FIRST_VALUE(units) IGNORE NULLS OVER (PARTITION BY product_id ORDER BY sale_date_plus_84d, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_units,
        MIN(units) OVER (PARTITION BY product_id) AS product_id_min_units
    FROM base
    WHERE units BETWEEN 12 AND 13
),
latest AS (
    SELECT
        id,
        sale_date_plus_84d,
        product_id_min_units
    FROM (
        SELECT *, RANK() OVER (PARTITION BY product_id ORDER BY product_id_min_units) AS row_num
        FROM enriched
    ) t
    WHERE row_num = 1
),
scored AS (
    SELECT
        *,
        NTILE(10) OVER (ORDER BY product_id_min_units, id) AS product_id_min_units_bucket,
        DENSE_RANK() OVER w_hist AS product_id_min_units_dense_rank,
        NTILE(10) OVER (ORDER BY product_id_min_units DESC, id) AS product_id_min_units_bucket_2,
        FIRST_VALUE(product_id_min_units, TRUE) OVER (ORDER BY sale_date_plus_84d, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_product_id_min_units
    FROM latest
    WINDOW w_hist AS (ORDER BY product_id_min_units DESC NULLS LAST)
),
enriched_store_sales AS (
    SELECT
        id,
        sale_date_plus_84d,
        product_id_min_units,
        product_id_min_units_dense_rank,
        first_known_product_id_min_units,
        LEAD(product_id_min_units, 1) OVER w_part AS next_product_id_min_units,
        LAST_VALUE(product_id_min_units_bucket, TRUE) OVER (ORDER BY product_id_min_units_bucket, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_product_id_min_units_bucket,
        LAG(first_known_product_id_min_units, 1, 0) OVER w_part AS prev_first_known_product_id_min_units
    FROM scored
    WINDOW w_part AS (ORDER BY product_id_min_units_bucket, id)
),
metrics AS (
    SELECT
        *,
        MAX(first_known_product_id_min_units) OVER (ORDER BY next_product_id_min_units, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_max_first_known_product_id_min_units,
        COUNT(first_known_product_id_min_units) OVER (ORDER BY next_product_id_min_units, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_count_first_known_product_id_min_units,
        AVG(first_known_product_id_min_units) OVER (ORDER BY next_product_id_min_units, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_avg_first_known_product_id_min_units,
        FIRST_VALUE(product_id_min_units) IGNORE NULLS OVER (ORDER BY last_known_product_id_min_units_bucket, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_product_id_min_units_2
    FROM enriched_store_sales
),
metrics_store_sales AS (
    SELECT
        *,
        LEAD(product_id_min_units_dense_rank) OVER w_ordered AS next_product_id_min_units_dense_rank,
        LAG(product_id_min_units_dense_rank) OVER w_ordered AS prev_product_id_min_units_dense_rank,
        LAG(product_id_min_units) OVER w_ordered AS prev_product_id_min_units,
        LEAD(product_id_min_units) OVER w_ordered AS next_product_id_min_units_2,
        ROW_NUMBER() OVER w_ordered AS event_seq,
        LAST_VALUE(product_id_min_units, TRUE) OVER (ORDER BY product_id_min_units_dense_rank, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_product_id_min_units
    FROM metrics
    WINDOW w_ordered AS (ORDER BY first_known_product_id_min_units, id)
)
SELECT
    DATE_TRUNC('YEAR', sale_date_plus_84d) AS year_start,
    DATE_TRUNC('QUARTER', sale_date_plus_84d) AS quarter_start,
    COUNT(*) AS num_rows,
    APPROX_PERCENTILE(event_seq, 0.9) AS event_seq_p90,
    AVG(running_avg_first_known_product_id_min_units) AS avg_running_avg_first_known_product_id_min_units,
    PERCENTILE_APPROX(next_product_id_min_units_dense_rank, ARRAY(0.25, 0.5, 0.75)) AS next_product_id_min_units_dense_rank_quartiles,
    APPROX_PERCENTILE(next_product_id_min_units, 0.5) AS next_product_id_min_units_p50,
    DENSE_RANK() OVER (PARTITION BY DATE_TRUNC('YEAR', sale_date_plus_84d) ORDER BY AVG(running_avg_first_known_product_id_min_units) DESC) AS avg_running_avg_first_known_product_id_min_units_dense_rank
FROM metrics_store_sales
GROUP BY DATE_TRUNC('YEAR', sale_date_plus_84d), DATE_TRUNC('QUARTER', sale_date_plus_84d)
ORDER BY quarter_start;
