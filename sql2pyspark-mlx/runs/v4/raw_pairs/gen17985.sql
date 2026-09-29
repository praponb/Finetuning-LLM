WITH cleaned AS (
    -- Filter the raw rows and derive helper columns
    SELECT *, UPPER(country) country_upper
    FROM suppliers
    WHERE country <> 'UK'
),
with_latest AS (
    -- Latest matching product per row (LATERAL ... LIMIT 1)
    SELECT s.*, x.product_stock_qty
    FROM cleaned s
    JOIN LATERAL (
            SELECT c.stock_qty AS product_stock_qty
            FROM products c
            WHERE c.supplier_id = s.id
            ORDER BY c.price DESC, c.id DESC
            LIMIT 1
        ) x ON TRUE
),
scored AS (
    -- Ranking and gap features
    SELECT
        id,
        supplier_name,
        country,
        city,
        rating,
        country_upper,
        product_stock_qty,
        AVG(rating) OVER (PARTITION BY country ORDER BY rating, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) rating_avg_2,
        FIRST_VALUE(rating) IGNORE NULLS OVER (ORDER BY product_stock_qty, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) first_known_rating,
        RANK() OVER (PARTITION BY city, country ORDER BY product_stock_qty DESC NULLS FIRST) product_stock_qty_rank
    FROM with_latest
),
ranked AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *
    FROM scored
    QUALIFY ROW_NUMBER() OVER (PARTITION BY country ORDER BY rating DESC NULLS FIRST, id) = 1
),
metrics AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        ROUND(rating_avg_2 / SUM(rating_avg_2) OVER (PARTITION BY country), 4) rating_avg_2_share,
        ROUND(rating / SUM(rating) OVER (PARTITION BY country, city), 4) rating_share,
        ROW_NUMBER() OVER (PARTITION BY city ORDER BY first_known_rating DESC, id) seq_no
    FROM ranked
)
-- Grouped metrics
SELECT
    CASE WHEN product_stock_qty_rank >= 7 THEN 'LARGE' ELSE 'SMALL' END AS product_stock_qty_rank_tier,
    city,
    SIZE(COLLECT_SET(city)) AS num_cities,
    SUM(CASE WHEN country = 'UK' THEN first_known_rating ELSE 0 END) AS uk_first_known_rating,
    COUNT(CASE WHEN country = 'SG' THEN 1 END) AS sg_count,
    COUNT(*) AS row_count
FROM metrics
GROUP BY CUBE (CASE WHEN product_stock_qty_rank >= 7 THEN 'LARGE' ELSE 'SMALL' END, city)
ORDER BY sg_count DESC;
