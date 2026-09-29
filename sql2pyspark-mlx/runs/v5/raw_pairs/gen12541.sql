WITH enriched AS (
    SELECT
        id,
        product_id,
        sale_date,
        revenue,
        ROW_NUMBER() OVER (PARTITION BY product_id ORDER BY units DESC, id) row_seq,
        LAST_VALUE(units) IGNORE NULLS OVER (PARTITION BY product_id ORDER BY revenue, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) last_known_units,
        AVG(units) OVER w_grp units_avg_3_prior,
        AVG(revenue) OVER w_grp revenue_avg_3_prior,
        AVG(revenue) OVER (PARTITION BY product_id) product_id_avg_revenue
    FROM store_sales
    WHERE store_id IN (3, 6)
    WINDOW w_grp AS (PARTITION BY product_id ORDER BY revenue, id ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING)
),
scored AS (
    SELECT
        *,
        SUM(row_seq) OVER (PARTITION BY product_id ORDER BY revenue_avg_3_prior, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) running_row_seq,
        PERCENT_RANK() OVER (PARTITION BY product_id ORDER BY revenue) revenue_percent_rank,
        AVG(units_avg_3_prior) OVER (ORDER BY units_avg_3_prior, id ROWS BETWEEN 5 PRECEDING AND CURRENT ROW) units_avg_3_prior_avg_5,
        RANK() OVER w_grp row_seq_rank,
        AVG(product_id_avg_revenue) OVER w_grp_2 product_id_avg_revenue_avg_2_prior
    FROM enriched
    WINDOW w_grp AS (PARTITION BY product_id ORDER BY row_seq), w_grp_2 AS (PARTITION BY product_id ORDER BY revenue, id ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING)
),
with_windows AS (
    SELECT
        id,
        product_id,
        row_seq,
        units_avg_3_prior,
        revenue_avg_3_prior,
        product_id_avg_revenue,
        running_row_seq,
        product_id_avg_revenue_avg_2_prior,
        DENSE_RANK() OVER (PARTITION BY product_id ORDER BY last_known_units DESC NULLS LAST) last_known_units_dense_rank,
        AVG(row_seq) OVER w_hist row_seq_avg_6_prior
    FROM scored
    WINDOW w_hist AS (PARTITION BY product_id ORDER BY running_row_seq, id ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING)
),
with_windows_store_sales AS (
    SELECT
        id,
        product_id_avg_revenue_avg_2_prior,
        last_known_units_dense_rank,
        row_seq_avg_6_prior,
        LAST_VALUE(revenue_avg_3_prior, TRUE) OVER (PARTITION BY product_id ORDER BY product_id_avg_revenue_avg_2_prior, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) last_known_revenue_avg_3_prior,
        ROW_NUMBER() OVER (PARTITION BY product_id ORDER BY product_id_avg_revenue_avg_2_prior, id) seq_no,
        COUNT(units_avg_3_prior) OVER (PARTITION BY product_id) product_id_count_units_avg_3_prior
    FROM with_windows
)
SELECT
    CASE WHEN last_known_units_dense_rank >= 7 THEN 'PREMIUM' ELSE 'STANDARD' END AS last_known_units_dense_rank_tier,
    CASE WHEN last_known_revenue_avg_3_prior >= 3647 THEN 'HIGH' ELSE 'LOW' END AS last_known_revenue_avg_3_prior_tier,
    ROUND(AVG(product_id_count_units_avg_3_prior), 2) AS avg_product_id_count_units_avg_3_prior,
    PERCENTILE_APPROX(product_id_count_units_avg_3_prior, 0.95) AS product_id_count_units_avg_3_prior_p95,
    COUNT(*) AS event_count,
    PERCENTILE_APPROX(last_known_units_dense_rank, ARRAY(0.25, 0.5, 0.75)) AS last_known_units_dense_rank_quartiles,
    ROUND(AVG(seq_no), 2) AS avg_seq_no,
    PERCENTILE_APPROX(last_known_revenue_avg_3_prior, ARRAY(0.25, 0.5, 0.75)) AS last_known_revenue_avg_3_prior_quartiles
FROM with_windows_store_sales
GROUP BY CASE WHEN last_known_units_dense_rank >= 7 THEN 'PREMIUM' ELSE 'STANDARD' END, CASE WHEN last_known_revenue_avg_3_prior >= 3647 THEN 'HIGH' ELSE 'LOW' END
HAVING COUNT(*) >= 1
ORDER BY avg_seq_no DESC;
