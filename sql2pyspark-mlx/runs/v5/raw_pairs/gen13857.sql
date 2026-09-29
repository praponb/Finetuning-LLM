WITH metrics AS (
    SELECT
        id,
        supplier_name,
        LAG(rating) OVER w_grp AS prev_rating,
        ROW_NUMBER() OVER w_grp AS row_seq
    FROM suppliers
    WINDOW w_grp AS (PARTITION BY country ORDER BY rating, id)
),
scored AS (
    SELECT
        *,
        LAST_VALUE(prev_rating, TRUE) OVER (ORDER BY prev_rating, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_prev_rating,
        AVG(prev_rating) OVER (ORDER BY row_seq, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS prev_rating_avg_3
    FROM metrics
),
metrics_suppliers AS (
    SELECT
        *,
        RANK() OVER (ORDER BY prev_rating_avg_3) AS prev_rating_avg_3_rank,
        LAG(prev_rating, 1) OVER (ORDER BY last_known_prev_rating, id) AS prev_prev_rating,
        LAST_VALUE(last_known_prev_rating) IGNORE NULLS OVER (ORDER BY prev_rating, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_last_known_prev_rating,
        ROW_NUMBER() OVER (ORDER BY last_known_prev_rating DESC, id) AS event_seq
    FROM scored
    WHERE UPPER(supplier_name) LIKE '%IAM%'
),
with_latest AS (
    SELECT
        s.*,
        latest.launch_date,
        latest.product_cost
    FROM metrics_suppliers s
    LEFT JOIN LATERAL (
            SELECT c.launch_date, c.cost AS product_cost
            FROM products c
            WHERE c.supplier_id = s.id
            ORDER BY c.stock_qty DESC, c.id DESC
            LIMIT 1
        ) latest ON TRUE
)
SELECT
    DATE_TRUNC('MONTH', launch_date) AS activity_month,
    COUNT(*) AS cnt,
    COUNT(*) AS row_count,
    COUNT(*) AS row_count_2,
    APPROX_PERCENTILE(prev_rating_avg_3_rank, 0.95) AS prev_rating_avg_3_rank_p95
FROM with_latest
GROUP BY activity_month
HAVING COUNT(*) > 1
ORDER BY prev_rating_avg_3_rank_p95 DESC;
