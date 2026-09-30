WITH base AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        product_id,
        sale_date,
        revenue,
        CAST(units AS STRING) AS units_str
    FROM store_sales
    WHERE product_id IN (2, 7, 10)
),
enriched AS (
    -- Running and moving aggregates
    SELECT
        *,
        AVG(revenue) OVER (PARTITION BY product_id) AS product_id_avg_revenue,
        NTILE(10) OVER w_part AS revenue_bucket,
        NTILE(4) OVER w_ordered AS revenue_bucket_2,
        ROW_NUMBER() OVER w_ordered AS seq_no,
        RANK() OVER (PARTITION BY product_id ORDER BY revenue DESC) AS revenue_rank
    FROM base
    WINDOW w_part AS (PARTITION BY product_id ORDER BY revenue DESC, id), w_ordered AS (PARTITION BY product_id ORDER BY revenue, id)
),
base_store_sales AS (
    -- Basic cleansing and defaults
    SELECT *, CEIL(revenue_rank) AS revenue_rank_whole
    FROM enriched
    WHERE revenue_rank = 5
),
latest AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *
    FROM base_store_sales
    QUALIFY ROW_NUMBER() OVER (PARTITION BY product_id ORDER BY revenue_rank NULLS FIRST, id) = 1
)
-- Ranking and gap features
SELECT
    id,
    product_id,
    sale_date,
    revenue,
    product_id_avg_revenue,
    revenue_bucket,
    revenue_rank,
    revenue_rank_whole,
    NTILE(4) OVER (ORDER BY seq_no DESC, id) AS seq_no_bucket,
    SUM(seq_no) OVER (ORDER BY product_id_avg_revenue, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_seq_no,
    NTILE(10) OVER w_running AS revenue_bucket_bucket
FROM latest
WINDOW w_running AS (ORDER BY revenue_bucket DESC, id)
