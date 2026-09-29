WITH valid AS (
    -- Keep recent, valid rows
    SELECT
        id,
        product_name,
        category,
        brand,
        price,
        launch_date,
        tags,
        supplier_id,
        COALESCE(price, 0) AS price_filled
    FROM products
    WHERE launch_date >= DATE '2023-03-08'
      AND supplier_id IS NOT NULL
),
scored AS (
    -- Ranking and gap features
    SELECT
        id,
        category,
        brand,
        tags,
        price_filled,
        MAX(price_filled) OVER w_ordered AS brand_max_price_filled,
        ROW_NUMBER() OVER (PARTITION BY brand ORDER BY launch_date DESC, id) AS row_seq,
        AVG(price) OVER (PARTITION BY supplier_id, category ORDER BY price_filled, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS price_avg_2,
        AVG(price) OVER (ORDER BY launch_date, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS price_avg_6
    FROM valid
    WHERE MONTH(launch_date) IN (3, 10, 11)
    WINDOW w_ordered AS (PARTITION BY brand)
),
enriched AS (
    -- Ranking and gap features
    SELECT
        *,
        RANK() OVER (PARTITION BY category ORDER BY price_avg_2 DESC NULLS FIRST) AS price_avg_2_rank,
        SUM(row_seq) OVER (PARTITION BY brand ORDER BY price_avg_2, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_row_seq,
        AVG(price_avg_2) OVER (PARTITION BY category ORDER BY price_avg_6, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS price_avg_2_avg_3,
        MIN(price_avg_6) OVER (PARTITION BY category, brand) AS category_min_price_avg_6,
        SUM(row_seq) OVER (ORDER BY brand_max_price_filled, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_row_seq
    FROM scored
),
base AS (
    -- Keep recent, valid rows
    SELECT
        id,
        brand,
        price_filled,
        brand_max_price_filled,
        row_seq,
        price_avg_2,
        price_avg_6,
        running_row_seq,
        price_avg_2_avg_3,
        cumulative_row_seq,
        cumulative_row_seq - price_avg_2_rank AS cumulative_row_seq_minus_price_avg_2_rank,
        COALESCE(brand, 'DIRECT') AS resolved_brand
    FROM enriched
    WHERE brand IS DISTINCT FROM 'Globex'
),
metrics AS (
    -- Running and moving aggregates
    SELECT
        *,
        SUM(running_row_seq) OVER (ORDER BY price_avg_2, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_running_row_seq,
        LAST_VALUE(cumulative_row_seq_minus_price_avg_2_rank, TRUE) OVER (PARTITION BY brand ORDER BY running_row_seq, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_cumulative_row_seq_minus_price_avg_2_rank,
        STDDEV_SAMP(running_row_seq) OVER (PARTITION BY brand) AS brand_stddev_samp_running_row_seq,
        CUME_DIST() OVER (PARTITION BY brand ORDER BY cumulative_row_seq DESC NULLS LAST) AS cumulative_row_seq_cume_dist,
        NTILE(3) OVER (PARTITION BY brand ORDER BY price_avg_6, id) AS price_avg_6_bucket
    FROM base
),
with_last AS (
    -- Latest matching store_sale per row (LATERAL ... LIMIT 1)
    SELECT
        s.id,
        s.brand,
        s.price_filled,
        s.price_avg_6,
        s.running_row_seq,
        s.cumulative_row_seq,
        s.resolved_brand,
        s.last_known_cumulative_row_seq_minus_price_avg_2_rank,
        s.brand_stddev_samp_running_row_seq,
        latest.revenue
    FROM metrics s
    JOIN LATERAL (
            SELECT c.revenue
            FROM store_sales c
            WHERE c.product_id = s.id
            ORDER BY c.sale_date DESC, c.id DESC
            LIMIT 1
        ) latest ON TRUE
)
SELECT
    id,
    brand,
    price_avg_6,
    running_row_seq,
    cumulative_row_seq,
    resolved_brand,
    brand_stddev_samp_running_row_seq,
    revenue
FROM with_last
ORDER BY brand_stddev_samp_running_row_seq;
