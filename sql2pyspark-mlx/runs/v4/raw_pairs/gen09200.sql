WITH eligible AS (
    SELECT s.*
    FROM suppliers s
    WHERE NOT EXISTS (
            SELECT 1
            FROM products c
            WHERE c.supplier_id = s.id
              AND c.supplier_id IS NOT NULL
        )
),
metrics AS (
    SELECT
        id,
        supplier_name,
        country,
        city,
        rating,
        MAX(rating) OVER w_part AS country_max_rating,
        LAG(rating, 1) OVER (PARTITION BY city, country ORDER BY rating, id) AS prev_rating,
        RANK() OVER w_recent AS rating_rank
    FROM eligible
    WINDOW w_part AS (PARTITION BY country), w_recent AS (PARTITION BY country ORDER BY rating NULLS LAST)
),
ranked AS (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY city, country ORDER BY rating DESC, id) AS rank_in_group
    FROM metrics
    QUALIFY rank_in_group = 1
)
SELECT
    id,
    supplier_name,
    country,
    rating,
    prev_rating,
    rating_rank,
    rank_in_group
FROM ranked
ORDER BY rank_in_group DESC NULLS LAST
