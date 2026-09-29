WITH valid AS (
    -- Filter the raw rows and derive helper columns
    SELECT *, CASE WHEN rating >= 4.4 THEN 'High' WHEN rating >= 1.5 THEN 'Medium' ELSE 'Low' END AS rating_band
    FROM suppliers
    WHERE city IS NOT NULL
      AND LENGTH(supplier_name) >= 10
),
enriched AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        LEAD(rating) OVER (PARTITION BY city ORDER BY rating, id) AS next_rating,
        ROW_NUMBER() OVER (PARTITION BY city ORDER BY rating, id) AS seq_no,
        SUM(rating) OVER (PARTITION BY city, country ORDER BY rating, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_rating
    FROM valid
),
metrics AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        AVG(next_rating) OVER (PARTITION BY city ORDER BY rating, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS next_rating_avg_2,
        AVG(seq_no) OVER (PARTITION BY city) AS city_avg_seq_no,
        ROUND(rating / SUM(rating) OVER (PARTITION BY country), 4) AS rating_share
    FROM enriched
),
ranked AS (
    -- Keep one row per key (latest first)
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY country, city ORDER BY rating_share DESC, id) AS rn
        FROM metrics
    ) x
    WHERE rn = 1
),
latest AS (
    -- QUALIFY-style window filter
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY country ORDER BY seq_no, id) AS rank_in_group
        FROM ranked
    ) x
    WHERE rank_in_group = 1
)
-- Native SQL UNPIVOT (wide to long)
SELECT *
FROM (
    SELECT
        id,
        country,
        rating_share,
        rating
    FROM latest
) src
UNPIVOT (
    value FOR kpi IN (rating_share AS RATING_SHARE, rating AS RATING)
)
