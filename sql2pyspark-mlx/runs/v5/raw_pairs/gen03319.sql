WITH enriched AS (
    SELECT
        id,
        supplier_name,
        country,
        city,
        MAX(rating) OVER w_grp AS last4_max_rating,
        COUNT(rating) OVER w_grp AS last4_count_rating,
        MIN(rating) OVER w_grp AS last4_min_rating,
        AVG(rating) OVER w_grp_2 AS rating_avg_6_prior
    FROM suppliers
    WINDOW w_grp AS (PARTITION BY city ORDER BY rating ASC, id ASC ROWS BETWEEN 3 PRECEDING AND CURRENT ROW), w_grp_2 AS (PARTITION BY country ORDER BY rating ASC, id ASC ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING)
),
with_windows AS (
    SELECT
        *,
        LEAD(last4_min_rating, 2, 0) OVER (PARTITION BY country ORDER BY last4_min_rating ASC, id ASC) AS next_last4_min_rating,
        CUME_DIST() OVER (PARTITION BY city ORDER BY last4_min_rating DESC) AS last4_min_rating_cume_dist,
        ROUND(last4_min_rating / SUM(last4_min_rating) OVER (PARTITION BY country), 4) AS last4_min_rating_share
    FROM enriched
),
with_windows_suppliers AS (
    SELECT
        *,
        AVG(last4_count_rating) OVER w_hist AS running_avg_last4_count_rating,
        MIN(last4_count_rating) OVER w_hist AS running_min_last4_count_rating,
        COUNT(last4_count_rating) OVER w_hist AS running_count_last4_count_rating,
        SUM(last4_count_rating) OVER w_hist AS running_sum_last4_count_rating,
        FIRST_VALUE(last4_max_rating) IGNORE NULLS OVER (PARTITION BY city ORDER BY last4_max_rating ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_last4_max_rating
    FROM with_windows
    WINDOW w_hist AS (PARTITION BY city, country ORDER BY next_last4_min_rating ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
),
enriched_suppliers AS (
    SELECT
        id,
        last4_max_rating,
        rating_avg_6_prior,
        running_min_last4_count_rating,
        first_known_last4_max_rating,
        SUM(last4_count_rating) OVER (PARTITION BY city, country ORDER BY first_known_last4_max_rating ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_last4_count_rating,
        NTILE(3) OVER (PARTITION BY country ORDER BY next_last4_min_rating ASC, id ASC) AS next_last4_min_rating_bucket,
        ROUND(last4_max_rating / SUM(last4_max_rating) OVER (PARTITION BY city), 4) AS last4_max_rating_share
    FROM with_windows_suppliers
)
SELECT
    CASE WHEN first_known_last4_max_rating >= 4.1 THEN 'HIGH' ELSE 'LOW' END AS first_known_last4_max_rating_tier,
    COUNT(DISTINCT id) AS distinct_ids,
    PERCENTILE_APPROX(running_min_last4_count_rating, ARRAY(0.25, 0.5, 0.75)) AS running_min_last4_count_rating_quartiles,
    AVG(running_last4_count_rating) AS avg_running_last4_count_rating,
    ROUND(AVG(first_known_last4_max_rating), 2) AS avg_first_known_last4_max_rating
FROM enriched_suppliers
GROUP BY CASE WHEN first_known_last4_max_rating >= 4.1 THEN 'HIGH' ELSE 'LOW' END
HAVING distinct_ids > 1
ORDER BY distinct_ids DESC
