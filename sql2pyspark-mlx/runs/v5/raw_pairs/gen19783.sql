WITH deduped AS (
    -- QUALIFY-style window filter
    SELECT *
    FROM (
        SELECT *, RANK() OVER (PARTITION BY city ORDER BY rating NULLS LAST) AS row_num
        FROM suppliers
    ) ranked
    WHERE row_num = 1
),
with_windows AS (
    -- Ranking and gap features
    SELECT
        *,
        ROW_NUMBER() OVER (PARTITION BY country, city ORDER BY row_num DESC, id) AS seq_no,
        FIRST_VALUE(city) IGNORE NULLS OVER (PARTITION BY city ORDER BY row_num, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_city,
        ROW_NUMBER() OVER (PARTITION BY country, city ORDER BY rating, id) AS event_seq
    FROM deduped
),
deduped_suppliers AS (
    -- Keep one row per key (latest first)
    SELECT
        id,
        supplier_name,
        country,
        city,
        rating,
        row_num,
        seq_no
    FROM with_windows
    QUALIFY ROW_NUMBER() OVER (PARTITION BY city ORDER BY row_num DESC, id) = 1
),
enriched AS (
    -- Running and moving aggregates
    SELECT
        *,
        LAG(seq_no) OVER (PARTITION BY city, country ORDER BY row_num, id) AS prev_seq_no,
        LEAD(rating) OVER (PARTITION BY city, country ORDER BY row_num, id) AS next_rating,
        LAG(rating) OVER (PARTITION BY city, country ORDER BY row_num, id) AS prev_rating,
        ROW_NUMBER() OVER (PARTITION BY city, country ORDER BY row_num, id) AS row_seq,
        SUM(rating) OVER (PARTITION BY country) AS country_sum_rating
    FROM deduped_suppliers
),
grouped AS (
    -- Aggregate with conditional metrics
    SELECT
        CASE WHEN prev_seq_no >= 8 THEN 'LARGE' ELSE 'SMALL' END AS prev_seq_no_tier,
        city,
        SUM(CASE WHEN city = 'New York' THEN next_rating ELSE 0 END) AS new_york_next_rating,
        COUNT(*) FILTER (WHERE country = 'TH') AS th_rows,
        PERCENTILE_APPROX(rating, 0.95) AS rating_p95,
        COUNT_IF(country = 'UK') AS uk_events
    FROM enriched
    GROUP BY CASE WHEN prev_seq_no >= 8 THEN 'LARGE' ELSE 'SMALL' END, city
    HAVING th_rows > 1
)
SELECT
    prev_seq_no_tier,
    city,
    th_rows,
    rating_p95,
    uk_events
FROM grouped
ORDER BY rating_p95 NULLS FIRST;
