WITH latest AS (
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY city, country ORDER BY rating DESC, id) AS row_num
        FROM suppliers
    ) ranked
    WHERE row_num = 1
),
with_windows AS (
    SELECT
        id,
        supplier_name,
        country,
        city,
        rating,
        row_num,
        AVG(rating) OVER w_part AS rating_avg_5_prior,
        AVG(rating) OVER (PARTITION BY city, country) AS city_avg_rating,
        LEAD(rating, 1) OVER (PARTITION BY city ORDER BY row_num, id) AS next_rating,
        ROUND(rating / SUM(rating) OVER (PARTITION BY city), 4) AS rating_share,
        LAST_VALUE(city) IGNORE NULLS OVER (PARTITION BY country, city ORDER BY rating, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_city
    FROM latest
    WINDOW w_part AS (ORDER BY rating, id ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING)
),
filtered AS (
    SELECT
        *,
        ROUND(rating, 2) AS rating_rounded,
        COALESCE(rating, 0) AS rating_filled
    FROM with_windows
    WHERE country IN ('US', 'JP')
),
kept AS (
    SELECT s.*
    FROM filtered s
    WHERE NOT EXISTS (
            SELECT 1
            FROM products ch
            WHERE ch.supplier_id = s.id
              AND ch.category IN ('Home', 'Grocery', 'Fashion')
        )
),
with_windows_suppliers AS (
    SELECT
        *,
        LEAD(rating_rounded, 1) OVER (PARTITION BY city ORDER BY rating_rounded, id) AS next_rating_rounded,
        LAG(rating_filled, 2) OVER (PARTITION BY country ORDER BY rating_rounded, id) AS prev_rating_filled,
        SUM(city_avg_rating) OVER (ORDER BY rating_avg_5_prior, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_city_avg_rating,
        SUM(rating_avg_5_prior) OVER (ORDER BY city_avg_rating, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_rating_avg_5_prior
    FROM kept
),
metrics AS (
    SELECT
        *,
        ROW_NUMBER() OVER (ORDER BY next_rating_rounded DESC, id) AS event_seq,
        DENSE_RANK() OVER w_ordered AS rating_share_dense_rank
    FROM with_windows_suppliers
    WINDOW w_ordered AS (PARTITION BY country, city ORDER BY rating_share DESC NULLS FIRST)
),
wide AS (
    SELECT *
    FROM (
        SELECT
            city,
            country,
            event_seq
        FROM metrics
    ) src
    PIVOT (
        AVG(event_seq) AS total,
        COUNT(1) AS n
        FOR country IN ('US', 'UK')
    )
)
SELECT
    city,
    US_total,
    UK_total
FROM wide
