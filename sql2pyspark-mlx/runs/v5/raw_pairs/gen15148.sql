WITH with_last AS (
    -- Latest matching product per row (LATERAL ... LIMIT 1)
    SELECT
        s.*,
        top1.product_price,
        top1.product_stock_qty
    FROM suppliers s
    LEFT JOIN LATERAL (
            SELECT c.price AS product_price, c.stock_qty AS product_stock_qty
            FROM products c
            WHERE c.supplier_id = s.id
            ORDER BY c.price DESC, c.id DESC
            LIMIT 1
        ) top1 ON TRUE
),
not_flagged AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM with_last s
    WHERE NOT EXISTS (
            SELECT 1
            FROM products ch
            WHERE ch.supplier_id = s.id
              AND ch.cost BETWEEN 390 AND 580
        )
),
eligible AS (
    -- Semi-join via a correlated subquery
    SELECT s.*
    FROM not_flagged s
    WHERE EXISTS (
            SELECT 1
            FROM products c
            WHERE c.supplier_id = s.id
              AND c.category IN ('Electronics', 'Grocery')
        )
),
latest AS (
    -- Keep one row per key (latest first)
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY city ORDER BY product_stock_qty DESC NULLS LAST, id) AS dedup_rank
        FROM eligible
    ) t
    WHERE dedup_rank = 1
),
scored AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        AVG(product_stock_qty) OVER (PARTITION BY city ORDER BY dedup_rank, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS product_stock_qty_avg_3,
        CUME_DIST() OVER (PARTITION BY country ORDER BY rating DESC) AS rating_cume_dist,
        SUM(product_price) OVER (PARTITION BY city, country) AS city_sum_product_price,
        NTILE(10) OVER (PARTITION BY country, city ORDER BY product_stock_qty, id) AS product_stock_qty_bucket
    FROM latest
    WHERE supplier_name IS NOT NULL
),
enriched AS (
    -- Ranking and gap features
    SELECT
        *,
        DENSE_RANK() OVER (ORDER BY product_price) AS product_price_dense_rank,
        LEAD(product_stock_qty_avg_3, 2) OVER (PARTITION BY city ORDER BY rating, id) AS next_product_stock_qty_avg_3,
        NTILE(5) OVER (PARTITION BY country, city ORDER BY product_stock_qty, id) AS product_stock_qty_bucket_2
    FROM scored
)
-- Aggregate with conditional metrics
SELECT
    CASE WHEN city_sum_product_price >= 5601 THEN 'LARGE' ELSE 'SMALL' END AS city_sum_product_price_tier,
    COUNT(CASE WHEN city = 'Singapore' THEN 1 END) AS singapore_count,
    COUNT(*) FILTER (WHERE country = 'JP') AS jp_rows,
    AVG(product_stock_qty_bucket_2) AS avg_product_stock_qty_bucket_2,
    ROUND(SUM(product_price_dense_rank) / NULLIF(COUNT(CASE WHEN city = 'Chiang Mai' THEN 1 END), 0), 2) AS product_price_dense_rank_per_chiang_mai
FROM enriched
GROUP BY city_sum_product_price_tier
HAVING COUNT(CASE WHEN city = 'Singapore' THEN 1 END) >= 1
ORDER BY avg_product_stock_qty_bucket_2 DESC;
