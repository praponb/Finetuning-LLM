WITH valid AS (
    -- Keep recent, valid rows
    SELECT
        id,
        supplier_name,
        country,
        city,
        rating,
        COALESCE(supplier_name, 'Unknown') AS supplier_name_filled
    FROM suppliers
    WHERE city IS NOT NULL
),
enriched AS (
    -- Running and moving aggregates
    SELECT
        *,
        SUM(rating) OVER w_recent AS running_rating,
        SUM(rating) OVER w_recent AS running_rating_2,
        AVG(rating) OVER (PARTITION BY city, country ORDER BY rating, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS rating_avg_2,
        LEAD(rating, 1) OVER (PARTITION BY country ORDER BY rating, id) AS next_rating
    FROM valid
    WINDOW w_recent AS (PARTITION BY country ORDER BY rating, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
),
enriched_suppliers AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        MAX(running_rating_2) OVER w_hist AS running_max_running_rating_2,
        COUNT(running_rating_2) OVER w_hist AS running_count_running_rating_2,
        AVG(running_rating_2) OVER w_hist AS running_avg_running_rating_2,
        SUM(running_rating_2) OVER w_hist AS running_sum_running_rating_2
    FROM enriched
    WHERE rating <= 4.0
    WINDOW w_hist AS (PARTITION BY country, city ORDER BY rating, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
),
deduped AS (
    -- Keep one row per key (latest first)
    SELECT *, ROW_NUMBER() OVER (PARTITION BY country ORDER BY running_max_running_rating_2 DESC, id) AS dedup_rank
    FROM enriched_suppliers
    QUALIFY dedup_rank = 1
),
daily AS (
    -- Grouped metrics
    SELECT
        CASE WHEN rating >= 3.6 THEN 'PREMIUM' ELSE 'STANDARD' END AS rating_tier,
        city,
        COUNT(*) AS row_count,
        COUNT(*) AS row_count_2,
        PERCENTILE_APPROX(running_avg_running_rating_2, ARRAY(0.25, 0.5, 0.75)) AS running_avg_running_rating_2_quartiles
    FROM deduped
    GROUP BY rating_tier, city
)
SELECT
    rating_tier,
    city,
    running_avg_running_rating_2_quartiles
FROM daily
ORDER BY rating_tier DESC NULLS FIRST;
