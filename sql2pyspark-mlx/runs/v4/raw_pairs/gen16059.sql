WITH cleaned AS (
    -- Keep recent, valid rows
    SELECT
        id,
        store_name,
        city,
        opened_date,
        sqft,
        LENGTH(region) AS region_length
    FROM stores
    WHERE YEAR(opened_date) >= 2023
),
eligible AS (
    -- Semi-join via a correlated subquery
    SELECT s.*
    FROM cleaned s
    WHERE EXISTS (
            SELECT 1
            FROM store_sales ch
            WHERE ch.store_id = s.id
              AND ch.sale_date < s.opened_date
        )
),
enriched AS (
    -- Running and moving aggregates
    SELECT
        *,
        AVG(sqft) OVER (PARTITION BY city ORDER BY sqft, id ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING) AS sqft_avg_5_prior,
        CUME_DIST() OVER w_part AS region_length_cume_dist,
        FIRST_VALUE(sqft, TRUE) OVER (PARTITION BY city ORDER BY sqft, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_sqft
    FROM eligible
    WINDOW w_part AS (PARTITION BY city ORDER BY region_length DESC NULLS LAST)
),
enriched_stores AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        SUM(region_length_cume_dist) OVER (PARTITION BY city) AS city_sum_region_length_cume_dist,
        FIRST_VALUE(sqft, TRUE) OVER w_part AS first_known_sqft_2,
        NTILE(3) OVER w_grp AS sqft_bucket,
        LEAD(region_length_cume_dist, 1) OVER (ORDER BY first_known_sqft, id) AS next_region_length_cume_dist
    FROM enriched
    WINDOW w_part AS (PARTITION BY city ORDER BY opened_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW), w_grp AS (PARTITION BY city ORDER BY sqft DESC, id)
)
-- Aggregate with conditional metrics
SELECT
    city,
    APPROX_PERCENTILE(sqft, ARRAY(0.25, 0.5, 0.75)) AS sqft_quartiles,
    COUNT(*) AS event_count,
    COUNT(*) FILTER (WHERE city = 'Tokyo') AS tokyo_rows
FROM enriched_stores
GROUP BY city
HAVING COUNT(*) < 1;
