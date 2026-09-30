WITH with_latest AS (
    -- Latest matching inventory per row (LATERAL ... LIMIT 1)
    SELECT s.*, x.last_restock
    FROM stores s
    JOIN LATERAL (
            SELECT c.last_restock
            FROM inventory c
            WHERE c.store_id = s.id
            ORDER BY c.last_restock DESC, c.id DESC
            LIMIT 1
        ) x ON TRUE
),
enriched AS (
    -- Running and moving aggregates
    SELECT
        *,
        AVG(sqft) OVER (ORDER BY opened_date, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) sqft_avg_6,
        ROW_NUMBER() OVER (PARTITION BY region ORDER BY last_restock, id) seq_no,
        NTILE(5) OVER w_recent sqft_bucket,
        LAG(sqft, 2) OVER (PARTITION BY city ORDER BY sqft, id) prev_sqft,
        SUM(sqft) OVER (PARTITION BY region ORDER BY sqft, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) cumulative_sqft
    FROM with_latest
    WINDOW w_recent AS (ORDER BY sqft DESC, id)
),
with_last AS (
    -- Latest matching store_sale per row (LATERAL ... LIMIT 1)
    SELECT s.*, latest.sale_date
    FROM enriched s
    JOIN LATERAL (
            SELECT c.sale_date
            FROM store_sales c
            WHERE c.store_id = s.id
            ORDER BY c.revenue DESC, c.id DESC
            LIMIT 1
        ) latest ON TRUE
)
SELECT
    id,
    store_name,
    region,
    opened_date,
    sqft,
    last_restock,
    sqft_avg_6,
    seq_no,
    sqft_bucket
FROM with_last
