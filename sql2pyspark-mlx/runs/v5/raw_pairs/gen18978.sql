WITH filtered AS (
    -- Basic cleansing and defaults
    SELECT *, ROUND(rating, 1) AS rating_rounded
    FROM suppliers
    WHERE city IS DISTINCT FROM 'Singapore'
      AND city IN ('Berlin', 'Chiang Mai')
),
latest AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *, ROW_NUMBER() OVER (PARTITION BY city, country ORDER BY rating ASC NULLS FIRST, id ASC) AS row_num
    FROM filtered
    QUALIFY row_num = 1
),
cleaned AS (
    -- Filter the raw rows and derive helper columns
    SELECT *, UPPER(TRIM(supplier_name)) AS supplier_name_normalized
    FROM latest
    WHERE rating < 4.1
),
metrics AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        PERCENT_RANK() OVER (PARTITION BY country ORDER BY rating_rounded DESC NULLS FIRST) AS rating_rounded_percent_rank,
        FIRST_VALUE(city) IGNORE NULLS OVER (PARTITION BY city ORDER BY rating ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_city
    FROM cleaned
),
scored AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        supplier_name,
        country,
        city,
        rating,
        rating_rounded,
        row_num,
        supplier_name_normalized,
        rating_rounded_percent_rank,
        first_known_city,
        LAG(rating) OVER (PARTITION BY country ORDER BY rating_rounded ASC, id ASC) AS prev_rating,
        LEAD(rating) OVER (PARTITION BY country ORDER BY rating_rounded ASC, id ASC) AS next_rating,
        LAG(rating_rounded) OVER (PARTITION BY country ORDER BY rating_rounded ASC, id ASC) AS prev_rating_rounded,
        LEAD(rating_rounded) OVER (PARTITION BY country ORDER BY rating_rounded ASC, id ASC) AS next_rating_rounded,
        ROW_NUMBER() OVER (PARTITION BY country ORDER BY rating_rounded ASC, id ASC) AS event_seq
    FROM metrics
),
daily AS (
    -- Aggregate with conditional metrics
    SELECT
        country,
        city,
        ROUND(SUM(prev_rating_rounded) / NULLIF(COUNT(CASE WHEN country = 'SG' THEN 1 END), 0), 2) AS prev_rating_rounded_per_sg,
        ROUND(AVG(rating_rounded_percent_rank), 2) AS avg_rating_rounded_percent_rank,
        ROUND(SUM(rating_rounded_percent_rank) / NULLIF(COUNT(CASE WHEN country = 'SG' THEN 1 END), 0), 2) AS rating_rounded_percent_rank_per_sg,
        SUM(CASE WHEN city = 'Berlin' THEN next_rating_rounded ELSE 0 END) AS berlin_next_rating_rounded,
        ROUND(SUM(prev_rating) / NULLIF(COUNT(CASE WHEN country = 'SG' THEN 1 END), 0), 2) AS prev_rating_per_sg,
        GROUPING(country) AS grouping_id
    FROM scored
    GROUP BY ROLLUP (country, city)
    HAVING ROUND(AVG(rating_rounded_percent_rank), 2) > 0
)
SELECT
    country,
    avg_rating_rounded_percent_rank,
    rating_rounded_percent_rank_per_sg,
    berlin_next_rating_rounded,
    prev_rating_per_sg,
    grouping_id
FROM daily
ORDER BY rating_rounded_percent_rank_per_sg ASC, avg_rating_rounded_percent_rank DESC;
