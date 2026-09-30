WITH base AS (
    -- Filter the raw rows and derive helper columns
    SELECT *, LENGTH(supplier_name) AS supplier_name_length
    FROM suppliers
    WHERE city IS NOT NULL
      AND country IN ('JP', 'UK')
),
latest AS (
    -- QUALIFY-style window filter
    SELECT *, RANK() OVER (PARTITION BY city ORDER BY supplier_name_length DESC NULLS LAST) AS latest_rank
    FROM base
    QUALIFY latest_rank = 1
),
enriched AS (
    -- Ranking and gap features
    SELECT
        *,
        SUM(rating) OVER w_ordered AS running_rating,
        SUM(rating) OVER (PARTITION BY country, city ORDER BY latest_rank, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_rating
    FROM latest
    WHERE rating <= 3.9
    WINDOW w_ordered AS (PARTITION BY country, city ORDER BY rating, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        *,
        LEAD(supplier_name_length) OVER w_recent AS next_supplier_name_length,
        LEAD(cumulative_rating) OVER w_recent AS next_cumulative_rating,
        LAG(cumulative_rating) OVER w_recent AS prev_cumulative_rating,
        ROW_NUMBER() OVER w_recent AS seq_no
    FROM enriched
    WINDOW w_recent AS (PARTITION BY city, country ORDER BY running_rating, id)
)
-- Grouped metrics
SELECT
    country,
    CASE WHEN rating >= 3.5 THEN 'LARGE' ELSE 'SMALL' END AS rating_tier,
    PERCENTILE_APPROX(running_rating, ARRAY(0.25, 0.5, 0.75)) AS running_rating_quartiles,
    COUNT(DISTINCT country) AS distinct_countries,
    COUNT(CASE WHEN city = 'Chiang Mai' THEN 1 END) AS chiang_mai_count,
    COUNT(DISTINCT city) AS distinct_cities,
    ROUND(SUM(prev_cumulative_rating) / NULLIF(COUNT(CASE WHEN country = 'TH' THEN 1 END), 0), 2) AS prev_cumulative_rating_per_th
FROM with_windows
GROUP BY GROUPING SETS ((country, CASE WHEN rating >= 3.5 THEN 'LARGE' ELSE 'SMALL' END), (country))
ORDER BY distinct_cities DESC NULLS FIRST
