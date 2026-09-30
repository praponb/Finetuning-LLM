WITH latest AS (
    -- QUALIFY-style window filter
    SELECT *
    FROM suppliers
    QUALIFY ROW_NUMBER() OVER (PARTITION BY country, city ORDER BY rating, id) = 1
),
with_windows AS (
    -- Ranking and gap features
    SELECT
        id,
        supplier_name,
        country,
        city,
        rating,
        FIRST_VALUE(rating) IGNORE NULLS OVER (ORDER BY rating, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_rating,
        LAG(rating, 1) OVER (PARTITION BY country ORDER BY rating, id) AS prev_rating,
        LEAD(rating, 1, 0) OVER (PARTITION BY city ORDER BY rating, id) AS next_rating
    FROM latest
),
with_windows_suppliers AS (
    -- Ranking and gap features
    SELECT
        *,
        AVG(rating) OVER (ORDER BY rating, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS rating_avg_3,
        AVG(rating) OVER (ORDER BY prev_rating, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS rating_avg_6
    FROM with_windows
),
with_last AS (
    -- Latest matching product per row (LATERAL ... LIMIT 1)
    SELECT
        s.*,
        latest.product_price,
        latest.product_brand
    FROM with_windows_suppliers s
    JOIN LATERAL (
            SELECT c.price AS product_price, c.brand AS product_brand
            FROM products c
            WHERE c.supplier_id = s.id
            ORDER BY c.launch_date DESC, c.id DESC
            LIMIT 1
        ) latest ON TRUE
),
deduped AS (
    -- QUALIFY-style window filter
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY city ORDER BY next_rating DESC, id) AS rank_in_group
        FROM with_last
    ) t
    WHERE rank_in_group = 1
),
scored AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        AVG(next_rating) OVER (PARTITION BY city ORDER BY rating_avg_3, id ROWS BETWEEN 5 PRECEDING AND CURRENT ROW) AS next_rating_avg_5,
        ROW_NUMBER() OVER (PARTITION BY product_brand ORDER BY next_rating DESC, id) AS row_seq,
        LAG(rating_avg_3, 1, 0) OVER (PARTITION BY product_brand ORDER BY rank_in_group, id) AS prev_rating_avg_3,
        ROW_NUMBER() OVER (PARTITION BY product_brand ORDER BY first_known_rating DESC, id) AS event_seq,
        SUM(next_rating) OVER (ORDER BY rating_avg_3, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_next_rating
    FROM deduped
),
eligible AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM scored s
    WHERE NOT EXISTS (
            SELECT 1
            FROM products c
            WHERE c.supplier_id = s.id
              AND c.brand NOT IN ('Globex', 'Umbrella', 'Acme')
        )
),
grouped AS (
    -- Grouped metrics
    SELECT
        country,
        CASE WHEN rating_avg_6 >= 4 THEN 'LARGE' ELSE 'SMALL' END AS rating_avg_6_tier,
        SORT_ARRAY(COLLECT_SET(country)) AS country_list,
        APPROX_PERCENTILE(first_known_rating, ARRAY(0.25, 0.5, 0.75)) AS first_known_rating_quartiles,
        COUNT(*) AS cnt
    FROM eligible
    GROUP BY 1, 2
)
SELECT
    country,
    rating_avg_6_tier,
    first_known_rating_quartiles
FROM grouped
