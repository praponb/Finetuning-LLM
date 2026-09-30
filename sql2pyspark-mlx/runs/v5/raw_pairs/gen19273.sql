WITH with_last AS (
    -- Latest matching product per row (LATERAL ... LIMIT 1)
    SELECT s.*, top1.price
    FROM suppliers s
    LEFT JOIN LATERAL (
            SELECT c.price
            FROM products c
            WHERE c.supplier_id = s.id
            ORDER BY c.cost DESC, c.id DESC
            LIMIT 1
        ) top1 ON TRUE
),
valid AS (
    -- Filter the raw rows and derive helper columns
    SELECT *, CASE WHEN city IN ('Tokyo', 'Chiang Mai') THEN 'Priority' ELSE 'Other' END AS city_group
    FROM with_last
    WHERE rating <= 3.1
),
ranked AS (
    -- Deduplicate with ROW_NUMBER
    SELECT
        id,
        supplier_name,
        country,
        city,
        rating,
        city_group
    FROM valid
    QUALIFY ROW_NUMBER() OVER (PARTITION BY city ORDER BY price DESC NULLS LAST, id) = 1
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        *,
        FIRST_VALUE(rating) IGNORE NULLS OVER (PARTITION BY city, country ORDER BY rating, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_rating,
        LEAD(rating, 1) OVER (ORDER BY rating, id) AS next_rating
    FROM ranked
    WHERE city NOT IN ('Chiang Mai', 'Berlin', 'New York')
),
kept AS (
    -- Semi-join via a correlated subquery
    SELECT s.*
    FROM with_windows s
    WHERE EXISTS (
            SELECT 1
            FROM products ch
            WHERE ch.supplier_id = s.id
              AND ch.supplier_id = 1
        )
),
totals AS (
    -- Multi-level aggregation
    SELECT
        CASE WHEN rating >= 4.2 THEN 'LARGE' ELSE 'SMALL' END AS rating_tier,
        AVG(first_known_rating) AS avg_first_known_rating,
        COUNT(*) FILTER (WHERE city = 'Bangkok') AS bangkok_rows,
        AVG(next_rating) AS avg_next_rating
    FROM kept
    GROUP BY 1
)
SELECT
    rating_tier,
    avg_first_known_rating,
    avg_next_rating
FROM totals
ORDER BY avg_first_known_rating DESC NULLS FIRST;
