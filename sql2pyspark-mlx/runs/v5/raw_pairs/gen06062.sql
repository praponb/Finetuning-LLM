WITH enriched AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        city,
        PERCENT_RANK() OVER (ORDER BY rating DESC) rating_percent_rank,
        ROW_NUMBER() OVER (PARTITION BY city ORDER BY rating DESC, id ASC) row_seq
    FROM suppliers
),
ranked AS (
    -- Keep one row per key (latest first)
    SELECT
        id,
        city,
        rating_percent_rank,
        row_seq
    FROM enriched
    QUALIFY RANK() OVER (PARTITION BY city ORDER BY rating_percent_rank DESC NULLS FIRST) = 1
),
kept AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM ranked s
    WHERE NOT EXISTS (
            SELECT 1
            FROM products ch
            WHERE ch.supplier_id = s.id
              AND ch.brand IN ('Initech', 'Stark')
        )
),
cleaned AS (
    -- Keep recent, valid rows
    SELECT
        *,
        LOWER(city) city_lower,
        COALESCE(city, 'UNKNOWN') city_filled
    FROM kept
    WHERE city <> 'London'
),
latest_match AS (
    -- Latest matching product per row (LATERAL ... LIMIT 1)
    SELECT
        s.id,
        s.city,
        s.rating_percent_rank,
        s.city_lower,
        x.product_category,
        x.price
    FROM cleaned s
    JOIN LATERAL (
            SELECT c.category AS product_category, c.price
            FROM products c
            WHERE c.supplier_id = s.id
            ORDER BY c.price DESC, c.id DESC
            LIMIT 1
        ) x ON TRUE
),
latest AS (
    -- Keep one row per key (latest first)
    SELECT *
    FROM latest_match
    QUALIFY ROW_NUMBER() OVER (PARTITION BY product_category ORDER BY rating_percent_rank DESC NULLS LAST, id ASC) = 1
),
with_windows AS (
    -- Ranking and gap features
    SELECT
        id,
        city,
        rating_percent_rank,
        city_lower,
        ROUND(price / SUM(price) OVER (PARTITION BY product_category, city), 4) price_share,
        DENSE_RANK() OVER w_grp price_dense_rank,
        SUM(rating_percent_rank) OVER (PARTITION BY product_category, city ORDER BY price ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) running_rating_percent_rank,
        LAST_VALUE(product_category, TRUE) OVER w_recent last_known_product_category,
        LEAD(price, 1) OVER (ORDER BY price ASC, id ASC) next_price
    FROM latest
    WINDOW w_grp AS (PARTITION BY city ORDER BY price DESC), w_recent AS (PARTITION BY product_category, city ORDER BY rating_percent_rank ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
)
SELECT
    id,
    city,
    city_lower,
    price_share,
    running_rating_percent_rank,
    last_known_product_category,
    next_price
FROM with_windows
ORDER BY running_rating_percent_rank DESC NULLS FIRST
