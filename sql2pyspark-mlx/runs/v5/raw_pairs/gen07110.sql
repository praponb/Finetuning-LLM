WITH valid AS (
    SELECT
        id,
        country,
        city,
        rating,
        CONCAT(country, ' - ', city) country_city,
        COALESCE(city, 'DIRECT') resolved_city
    FROM suppliers
    WHERE city IS NOT NULL
      AND rating < 2.6
),
eligible AS (
    SELECT s.*
    FROM valid s
    WHERE NOT EXISTS (
            SELECT 1
            FROM products c
            WHERE c.supplier_id = s.id
              AND c.category = 'Fashion'
        )
),
enriched AS (
    SELECT
        *,
        SUM(rating) OVER (PARTITION BY city ORDER BY rating, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) running_rating,
        ROW_NUMBER() OVER (PARTITION BY city ORDER BY rating, id) event_seq,
        LAG(rating, 2) OVER (PARTITION BY country ORDER BY rating, id) prev_rating
    FROM eligible
    WHERE rating >= 1.9
),
with_last AS (
    SELECT
        s.id,
        s.country,
        s.city,
        s.country_city,
        s.resolved_city,
        s.running_rating,
        s.prev_rating,
        latest.product_stock_qty
    FROM enriched s
    LEFT JOIN LATERAL (
            SELECT c.stock_qty AS product_stock_qty
            FROM products c
            WHERE c.supplier_id = s.id
            ORDER BY c.price DESC, c.id DESC
            LIMIT 1
        ) latest ON TRUE
),
enriched_suppliers AS (
    SELECT
        *,
        COUNT(running_rating) OVER (PARTITION BY country) country_count_running_rating,
        RANK() OVER (PARTITION BY country ORDER BY product_stock_qty DESC NULLS LAST) product_stock_qty_rank,
        CUME_DIST() OVER (PARTITION BY country ORDER BY running_rating NULLS LAST) running_rating_cume_dist,
        ROW_NUMBER() OVER (ORDER BY product_stock_qty DESC, id) row_seq
    FROM with_last
),
ranked AS (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY city ORDER BY product_stock_qty_rank DESC NULLS FIRST, id) AS rank_in_group
    FROM enriched_suppliers
    QUALIFY rank_in_group = 1
)
SELECT
    city,
    COUNT(CASE WHEN country = 'JP' THEN 1 END) AS jp_count,
    SIZE(COLLECT_SET(city)) AS num_cities,
    COUNT(CASE WHEN country = 'UK' THEN 1 END) AS uk_count
FROM ranked
GROUP BY city
HAVING num_cities >= 1;
