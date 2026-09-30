WITH scored AS (
    SELECT
        *,
        ROW_NUMBER() OVER (PARTITION BY product_id ORDER BY revenue, id) AS event_seq,
        LAG(units, 1) OVER (PARTITION BY store_id ORDER BY sale_date, id) AS prev_units,
        FIRST_VALUE(revenue) IGNORE NULLS OVER (ORDER BY units, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_revenue
    FROM store_sales
),
deduped AS (
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY product_id, store_id ORDER BY first_known_revenue, id) AS row_num
        FROM scored
    ) ranked
    WHERE row_num = 1
),
deduped_store_sales AS (
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY store_id, product_id ORDER BY row_num DESC, id) AS latest_rank
        FROM deduped
    ) ranked
    WHERE latest_rank = 1
),
latest AS (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY store_id ORDER BY row_num, id) AS rank_in_group
    FROM deduped_store_sales
    QUALIFY rank_in_group = 1
),
deduped_store_sales_2 AS (
    SELECT *
    FROM latest
    QUALIFY ROW_NUMBER() OVER (PARTITION BY product_id ORDER BY prev_units DESC, id) = 1
),
enriched AS (
    SELECT
        *,
        COUNT(event_seq) OVER (PARTITION BY store_id) AS store_id_count_event_seq,
        SUM(revenue) OVER (PARTITION BY product_id ORDER BY latest_rank, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_revenue,
        NTILE(4) OVER (ORDER BY prev_units DESC, id) AS prev_units_bucket,
        PERCENT_RANK() OVER (PARTITION BY product_id ORDER BY revenue DESC NULLS LAST) AS revenue_percent_rank
    FROM deduped_store_sales_2
)
SELECT
    DATE_TRUNC('WEEK', sale_date) AS week_start,
    product_id,
    MAX(event_seq) AS max_event_seq,
    ROUND(AVG(prev_units_bucket), 2) AS avg_prev_units_bucket,
    COUNT(*) AS row_count,
    GROUPING_ID() AS grouping_level
FROM enriched
GROUP BY ROLLUP (DATE_TRUNC('WEEK', sale_date), product_id)
HAVING COUNT(*) >= 1
ORDER BY max_event_seq DESC
