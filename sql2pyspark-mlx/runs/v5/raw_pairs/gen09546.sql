WITH base AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        supplier_name,
        country,
        city,
        rating,
        TRIM(country) AS clean_country
    FROM suppliers
    WHERE city IS NOT NULL
),
deduped AS (
    -- QUALIFY-style window filter
    SELECT
        id,
        supplier_name,
        city,
        rating,
        clean_country
    FROM base
    QUALIFY RANK() OVER (PARTITION BY city, country ORDER BY rating) = 1
),
scored AS (
    -- Ranking and gap features
    SELECT
        id,
        supplier_name,
        COUNT(rating) OVER w_running AS running_count_rating,
        MIN(rating) OVER w_running AS running_min_rating,
        AVG(rating) OVER w_running AS running_avg_rating,
        AVG(rating) OVER (PARTITION BY city ORDER BY rating, id ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING) AS rating_avg_3_prior
    FROM deduped
    WHERE city IN ('Berlin', 'Bangkok')
    WINDOW w_running AS (PARTITION BY city ORDER BY rating, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        id,
        running_count_rating,
        running_avg_rating,
        PERCENT_RANK() OVER (ORDER BY running_count_rating DESC) AS running_count_rating_percent_rank,
        LAST_VALUE(rating_avg_3_prior, TRUE) OVER (ORDER BY running_min_rating, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_rating_avg_3_prior,
        LAG(running_count_rating, 1) OVER (ORDER BY running_avg_rating, id) AS prev_running_count_rating,
        DENSE_RANK() OVER (ORDER BY running_avg_rating DESC NULLS FIRST) AS running_avg_rating_dense_rank
    FROM scored
),
metrics AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        running_count_rating,
        running_avg_rating,
        last_known_rating_avg_3_prior,
        prev_running_count_rating,
        MAX(running_avg_rating) OVER (ORDER BY running_count_rating_percent_rank, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS last7_max_running_avg_rating,
        MIN(running_avg_rating) OVER (ORDER BY running_count_rating_percent_rank, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS last7_min_running_avg_rating,
        COUNT(running_avg_rating) OVER (ORDER BY running_count_rating_percent_rank, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS last7_count_running_avg_rating
    FROM with_windows
)
SELECT
    id,
    running_count_rating,
    running_avg_rating,
    last7_min_running_avg_rating,
    last7_count_running_avg_rating
FROM metrics
ORDER BY running_avg_rating DESC NULLS LAST;
