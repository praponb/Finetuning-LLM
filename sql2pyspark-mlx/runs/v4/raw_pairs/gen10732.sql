WITH kept AS (
    SELECT s.*
    FROM suppliers s
    WHERE NOT EXISTS (
            SELECT 1
            FROM products c
            WHERE c.supplier_id = s.id
              AND c.price BETWEEN 460 AND 770
        )
),
eligible AS (
    SELECT s.*
    FROM kept s
    WHERE EXISTS (
            SELECT 1
            FROM products c
            WHERE c.supplier_id = s.id
              AND c.category = 'Fashion'
        )
),
metrics AS (
    SELECT
        *,
        SUM(rating) OVER (PARTITION BY city ORDER BY rating, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_rating,
        FIRST_VALUE(rating, TRUE) OVER w_ordered AS first_known_rating,
        LEAD(rating, 1) OVER (PARTITION BY city ORDER BY rating, id) AS next_rating
    FROM eligible
    WINDOW w_ordered AS (PARTITION BY country ORDER BY rating, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
),
with_last AS (
    SELECT
        s.*,
        x.price,
        x.product_stock_qty
    FROM metrics s
    LEFT JOIN LATERAL (
            SELECT c.price, c.stock_qty AS product_stock_qty
            FROM products c
            WHERE c.supplier_id = s.id
            ORDER BY c.stock_qty DESC, c.id DESC
            LIMIT 1
        ) x ON TRUE
),
not_flagged AS (
    SELECT s.*
    FROM with_last s
    WHERE NOT EXISTS (
            SELECT 1
            FROM products ch
            WHERE ch.supplier_id = s.id
              AND ch.brand = 'Initech'
        )
),
deduped AS (
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY country, city ORDER BY next_rating DESC NULLS FIRST, id) AS dedup_rank
        FROM not_flagged
    ) x
    WHERE dedup_rank = 1
)
SELECT
    country,
    COUNT(*) FILTER (WHERE city = 'Bangkok') AS bangkok_rows,
    COUNT(*) AS cnt,
    ROUND(SUM(next_rating) / NULLIF(COUNT(CASE WHEN city = 'London' THEN 1 END), 0), 2) AS next_rating_per_london,
    SORT_ARRAY(COLLECT_SET(city)) AS city_list,
    MIN(next_rating) AS min_next_rating,
    COUNT(DISTINCT country) AS distinct_countries
FROM deduped
GROUP BY country
ORDER BY next_rating_per_london NULLS LAST;
