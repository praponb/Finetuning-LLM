WITH with_last AS (
    -- Latest matching product per row (LATERAL ... LIMIT 1)
    SELECT
        s.*,
        x.category,
        x.product_brand
    FROM suppliers s
    JOIN LATERAL (
            SELECT c.category, c.brand AS product_brand
            FROM products c
            WHERE c.supplier_id = s.id
            ORDER BY c.stock_qty DESC, c.id DESC
            LIMIT 1
        ) x ON TRUE
),
with_windows AS (
    -- Ranking and gap features
    SELECT
        *,
        SUM(rating) OVER (PARTITION BY category ORDER BY rating, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_rating,
        PERCENT_RANK() OVER (ORDER BY rating DESC NULLS LAST) AS rating_percent_rank,
        ROUND(rating / SUM(rating) OVER (PARTITION BY city, country), 4) AS rating_share
    FROM with_last
),
scored AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        supplier_name,
        country,
        city,
        rating,
        category,
        product_brand,
        running_rating,
        rating_percent_rank,
        rating_share,
        FIRST_VALUE(rating_percent_rank, TRUE) OVER (PARTITION BY category ORDER BY running_rating, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_rating_percent_rank,
        ROUND(running_rating / SUM(running_rating) OVER (PARTITION BY country), 4) AS running_rating_share
    FROM with_windows
)
-- Aggregate with conditional metrics
SELECT
    product_brand,
    city,
    COUNT_IF(category = 'Home') AS home_events,
    SUM(CASE WHEN city = 'Tokyo' THEN rating ELSE 0 END) AS tokyo_rating,
    PERCENTILE_APPROX(rating, 0.5) AS rating_p50
FROM scored
GROUP BY GROUPING SETS ((product_brand, city), (product_brand));
