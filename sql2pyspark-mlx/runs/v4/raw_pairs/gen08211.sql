WITH filtered AS (
    -- Keep recent, valid rows
    SELECT
        *,
        CONCAT(product_name, ' - ', category) AS product_name_category,
        DATE_TRUNC('week', launch_date) AS launch_date_period
    FROM products
    WHERE supplier_id IN (5, 8, 15, 34)
),
scored AS (
    -- Running and moving aggregates
    SELECT
        *,
        NTILE(5) OVER (PARTITION BY supplier_id ORDER BY cost DESC, id) AS cost_bucket,
        AVG(price) OVER (PARTITION BY supplier_id, brand ORDER BY launch_date_period, id ROWS BETWEEN 5 PRECEDING AND CURRENT ROW) AS price_avg_5,
        PERCENT_RANK() OVER (PARTITION BY brand ORDER BY stock_qty DESC NULLS FIRST) AS stock_qty_percent_rank
    FROM filtered
),
enriched AS (
    -- Ranking and gap features
    SELECT
        *,
        ROW_NUMBER() OVER (PARTITION BY category, brand ORDER BY launch_date_period DESC, id) AS event_seq,
        DENSE_RANK() OVER (PARTITION BY category, supplier_id ORDER BY price DESC NULLS FIRST) AS price_dense_rank,
        LAST_VALUE(brand) IGNORE NULLS OVER (ORDER BY cost_bucket, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_brand,
        AVG(price_avg_5) OVER (ORDER BY price_avg_5, id ROWS BETWEEN 5 PRECEDING AND CURRENT ROW) AS price_avg_5_avg_5,
        SUM(stock_qty) OVER (ORDER BY launch_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_stock_qty
    FROM scored
)
SELECT
    id,
    product_name,
    brand,
    price,
    stock_qty,
    tags,
    product_name_category,
    launch_date_period,
    cost_bucket
FROM enriched
